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

  lazy val writePartitionCql =
    session.prepare(
      s"INSERT INTO ${tableString} (partKey, startTime, endTime) VALUES (?, ?, ?) USING TTL ?")
      .setConsistencyLevel(writeConsistencyLevel)

  lazy val writePartitionCqlNoTtl =
    session.prepare(
      s"INSERT INTO ${tableString} (partKey, startTime, endTime) VALUES (?, ?, ?)")
      .setConsistencyLevel(writeConsistencyLevel)

  def writePartKey(pk: PartKeyRecord, diskTimeToLive: Int): Future[Response] = {
    if (diskTimeToLive <= 0) {
      connector.execStmtWithRetries(writePartitionCqlNoTtl.bind(
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong))
    } else {
      connector.execStmtWithRetries(writePartitionCql.bind(
        toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong, diskTimeToLive: JInt))
    }
  }

  val scanCql = session.prepare(s"SELECT * FROM ${tableString} WHERE TOKEN(partKey) >= ? AND TOKEN(partKey) < ?")
  def scanPartKeys(tokens: Seq[(String, String)], shard: Int): Observable[PartKeyRecord] = {
    val res: Observable[Iterator[PartKeyRecord]] = Observable.fromIterable(tokens)
      .mapAsync { range =>
        val fut = session.executeAsync(scanCql.bind(range._1, range._2)).toIterator.handleErrors
                         .map { rowIt => rowIt.map(PartitionKeysTable.rowToPartKeyRecord) }
        Task.fromFuture(fut)
      }
    for {
      pkRecs <- res
      pk <- Observable.fromIterator(pkRecs)
    } yield pk
  }
}

object PartitionKeysTable {
  private[columnstore] def rowToPartKeyRecord(row: Row) = {
    PartKeyRecord(row.getBytes("partKey").array(),
      row.getLong("startTime"), row.getLong("endTime"), None)
  }
}
