package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Row}
import monix.eval.Task
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

sealed class PartitionKeysTable(val dataset: DatasetRef,
                                val shard: Int,
                                val connector: FiloCassandraConnector,
                                writeConsistencyLevel: ConsistencyLevel)
                               (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = s"partitionkeys_$shard"

  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    partKey blob,
       |    startTime bigint,
       |    endTime bigint,
       |    PRIMARY KEY (partKey)
       |) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writePartitionCql = session.prepare(
      s"INSERT INTO ${tableString} (partKey, startTime, endTime) " +
      s"VALUES (?, ?, ?) USING TTL ?")
      .setConsistencyLevel(writeConsistencyLevel)

  private lazy val writePartitionCqlNoTtl = session.prepare(
      s"INSERT INTO ${tableString} (partKey, startTime, endTime) " +
        s"VALUES (?, ?, ?)")
      .setConsistencyLevel(writeConsistencyLevel)

  private lazy val scanCql = session.prepare(
    s"SELECT * FROM $tableString " +
    s"WHERE TOKEN(partKey) >= ? AND TOKEN(partKey) < ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val deleteCql = session.prepare(
    s"DELETE FROM $tableString " +
    s"WHERE partKey = ?"
  )

  def writePartKey(pk: PartKeyRecord, diskTimeToLiveSeconds: Int): Future[Response] = {
    if (diskTimeToLiveSeconds <= 0) {
      connector.execStmtWithRetries(writePartitionCqlNoTtl.bind(
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong))
    } else {
      connector.execStmtWithRetries(writePartitionCql.bind(
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong, diskTimeToLiveSeconds: JInt))
    }
  }

  def scanPartKeys(tokens: Seq[(String, String)]): Observable[PartKeyRecord] = {
    val res: Observable[Iterator[PartKeyRecord]] = Observable.fromIterable(tokens)
      .mapAsync { range =>
        val fut = session.executeAsync(scanCql.bind(range._1.toLong: JLong, range._2.toLong: JLong))
                         .toIterator.handleErrors
                         .map { rowIt => rowIt.map(PartitionKeysTable.rowToPartKeyRecord) }
        Task.fromFuture(fut)
      }
    for {
      pkRecs <- res
      pk <- Observable.fromIterator(pkRecs)
    } yield pk
  }

  def deletePartKey(pk: Array[Byte], shard: Int): Future[Response] = {
    val  stmt = deleteCql.bind().setBytes(0, toBuffer(pk)).setConsistencyLevel(writeConsistencyLevel)
    connector.execStmtWithRetries(stmt)
  }

}

object PartitionKeysTable {
  private[columnstore] def rowToPartKeyRecord(row: Row) = {
    PartKeyRecord(row.getBytes("partKey").array(),
      row.getLong("startTime"), row.getLong("endTime"), None)
  }
}
