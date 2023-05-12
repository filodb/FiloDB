package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

import com.datastax.driver.core.{ConsistencyLevel, Row}
import monix.eval.Task
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

sealed class PartitionKeysV2Table(val dataset: DatasetRef,
                                val connector: FiloCassandraConnector,
                                writeConsistencyLevel: ConsistencyLevel,
                                readConsistencyLevel: ConsistencyLevel)
                               (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = s"partitionkeysv2"

  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    shard int,
       |    bucket int,
       |    partKey blob,
       |    startTime bigint,
       |    endTime bigint,
       |    PRIMARY KEY ((shard, bucket), partKey)
       |) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writePartitionCql = session.prepare(
      s"INSERT INTO ${tableString} (shard, bucket, partKey, startTime, endTime) " +
      s"VALUES (?, ?, ?, ?, ?) USING TTL ?")
      .setConsistencyLevel(writeConsistencyLevel)

  private lazy val writePartitionCqlNoTtl = session.prepare(
      s"INSERT INTO ${tableString} (shard, bucket, partKey, startTime, endTime) " +
        s"VALUES (?, ?, ?, ?, ?)")
      .setConsistencyLevel(writeConsistencyLevel)

  private lazy val scanCql = session.prepare(
    s"SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE shard = ? and bucket = ?"
  ).setConsistencyLevel(readConsistencyLevel)

  private lazy val scanCqlForStartEndTime = session.prepare(
    s"SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE TOKEN(shard, bucket) >= ? AND TOKEN(shard, bucket) < ? AND " +
      s"startTime >= ? AND startTime <= ? AND " +
      s"endTime >= ? AND endTime <= ? " +
      s"ALLOW FILTERING")
    .setConsistencyLevel(readConsistencyLevel)

  private lazy val scanCqlForStartTime = session.prepare(
    s"SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE TOKEN(shard, bucket) >= ? AND TOKEN(shard, bucket) < ? AND startTime >= ? AND startTime <= ? " +
      s"ALLOW FILTERING")
    .setConsistencyLevel(readConsistencyLevel)

  private lazy val scanCqlForEndTime = session.prepare(
    s"SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE TOKEN(shard, bucket) >= ? AND TOKEN(shard, bucket) < ? AND endTime >= ? AND endTime <= ? " +
      s"ALLOW FILTERING")
    .setConsistencyLevel(readConsistencyLevel)

  private lazy val readCql = session.prepare(
    s"SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE shard = ? and bucket = ? and partKey = ?")
    .setConsistencyLevel(readConsistencyLevel)

  private lazy val deleteCql = session.prepare(
    s"DELETE FROM $tableString " +
    s"WHERE shard = ? and bucket = ? and partKey = ?"
  ).setConsistencyLevel(writeConsistencyLevel)

  def writePartKey(bucket: Int, pk: PartKeyRecord, diskTimeToLiveSeconds: Long): Future[Response] = {
    if (diskTimeToLiveSeconds <= 0) {
      connector.execStmtWithRetries(writePartitionCqlNoTtl.bind(pk.shard: JInt, bucket: JInt,
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong))
    } else {
      connector.execStmtWithRetries(writePartitionCql.bind(pk.shard: JInt, bucket: JInt,
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong, diskTimeToLiveSeconds.toInt: JInt))
    }
  }

  def scanPartKeys(shard: Int, scanParallelism: Int, numBuckets: Int): Observable[PartKeyRecord] = {

    /*
    TODO If this is slow, then another option to evaluate is to do token scan with shard filter.
    SELECT partKey, startTime, endTime, shard FROM $tableString " +
      s"WHERE TOKEN(shard, bucket) >= ? AND TOKEN(shard, bucket) < ? AND " +
      s"shard = ? ALLOW FILTERING")
     */

    val res: Observable[Iterator[PartKeyRecord]] = Observable.fromIterable(0 until numBuckets)
      .mapParallelUnordered(scanParallelism) { bucket =>
        val fut = session.executeAsync(scanCql.bind(shard: JInt, bucket: JInt))
                         .toIterator.handleErrors
                         .map { rowIt => rowIt.map(PartitionKeysV2Table.rowToPartKeyRecord) }
        Task.fromFuture(fut)
      }
    for {
      pkRecs <- res
      pk <- Observable.fromIteratorUnsafe(pkRecs)
    } yield pk
  }

  /**
   * Method used by data repair jobs.
   * Return PartitionKey rows where timeSeries startTime falls within the specified repair start/end window.
   * (In other words - return timesSeries that were born during the data loss period)
   * Rows consist of partKey, start and end time. Refer the CQL used below.
   */
  def scanRowsByStartTimeRangeNoAsync(tokens: Seq[(String, String)],
                                      repairStartTime: Long,
                                      repairEndTime: Long): Set[Row] = {
    tokens.iterator.flatMap { case (start, end) =>
      /*
       * FIXME conversion of tokens to Long works only for Murmur3Partitioner because it generates
       * Long based tokens. If other partitioners are used, this can potentially break.
       * Correct way is to pass Token objects around and bind tokens with stmt.bind().setPartitionKeyToken(token)
       */
      val startTimeStmt = scanCqlForStartTime.bind(start.toLong: java.lang.Long,
        end.toLong: java.lang.Long,
        repairStartTime: java.lang.Long,
        repairEndTime: java.lang.Long)
      session.execute(startTimeStmt).iterator.asScala
    }
  }.toSet

  /**
   * Method used by data repair jobs.
   * Return PartitionKey rows where timeSeries endTime falls within the specified repair start/end window.
   * (In other words - return timesSeries that died during the data loss period)
   * Rows consist of partKey, start and end time. Refer the CQL used below.
   */
  def scanRowsByEndTimeRangeNoAsync(tokens: Seq[(String, String)],
                                    startTime: Long,
                                    endTime: Long): Set[Row] = {
    tokens.iterator.flatMap { case (start, end) =>
      /*
       * FIXME conversion of tokens to Long works only for Murmur3Partitioner because it generates
       * Long based tokens. If other partitioners are used, this can potentially break.
       * Correct way is to pass Token objects around and bind tokens with stmt.bind().setPartitionKeyToken(token)
       */
      val endTimeStmt = scanCqlForEndTime.bind(start.toLong: java.lang.Long,
        end.toLong: java.lang.Long,
        startTime: java.lang.Long,
        endTime: java.lang.Long)
      session.execute(endTimeStmt).iterator.asScala
    }
  }.toSet

  /**
   * Method used by Cardinality Buster job.
   * Return PartitionKeyRecord objects where timeSeries start and end Time falls within the specified window.
   */
  def scanPksByStartEndTimeRangeNoAsync(split: (String, String),
                                        startTimeGTE: Long,
                                        startTimeLTE: Long,
                                        endTimeGTE: Long,
                                        endTimeLTE: Long): Iterator[PartKeyRecord] = {
      /*
       * FIXME conversion of tokens to Long works only for Murmur3Partitioner because it generates
       * Long based tokens. If other partitioners are used, this can potentially break.
       * Correct way is to pass Token objects around and bind tokens with stmt.bind().setPartitionKeyToken(token)
       */
      val stmt = scanCqlForStartEndTime.bind(split._1.toLong: java.lang.Long,
        split._2.toLong: java.lang.Long,
        startTimeGTE: java.lang.Long,
        startTimeLTE: java.lang.Long,
        endTimeGTE: java.lang.Long,
        endTimeLTE: java.lang.Long)
    session.execute(stmt).iterator.asScala.map(PartitionKeysV2Table.rowToPartKeyRecord)
  }

  /**
   * Returns PartKeyRecord for a given partKey bytes.
   *
   * @param pk partKey bytes
   * @return Option[PartKeyRecord]
   */
  def readPartKey(shard: Int, bucket: Int, pk: Array[Byte]) : Option[PartKeyRecord] = {
    val iterator = session.execute(readCql.bind(shard: JInt, bucket: JInt, toBuffer(pk))).iterator()
    if (iterator.hasNext) {
      Some(PartitionKeysV2Table.rowToPartKeyRecord(iterator.next()))
    } else {
      None
    }
  }

  /**
   * Deletes PartKeyRecord for a given partKey bytes.
   *
   * @param pk partKey bytes
   * @return Future[Response]
   */
  def deletePartKeyNoAsync(shard: Int, bucket: Int, pk: Array[Byte]): Response = {
    val stmt = deleteCql.bind(shard: JInt, bucket: JInt, toBuffer(pk)).setConsistencyLevel(writeConsistencyLevel)
    connector.execCqlNoAsync(stmt)
  }

}

object PartitionKeysV2Table {
  private[columnstore] def rowToPartKeyRecord(row: Row) = {
    PartKeyRecord(row.getBytes("partKey").array(),
      row.getLong("startTime"), row.getLong("endTime"), row.getInt("shard"))
  }
}
