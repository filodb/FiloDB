package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
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

  def scanPartKeys(tokens: Seq[(String, String)], shard: Int): Observable[PartKeyRecord] = {
    def cql(start: String, end: String): String =
      s"SELECT * FROM ${tableString} " +
        s"WHERE TOKEN(partKey) >= $start AND TOKEN(partKey) < $end "
    val it = tokens.iterator.flatMap { case (start, end) =>
      session.execute(cql(start, end)).iterator.asScala
        .map { row => PartKeyRecord(row.getBytes("partKey").array(),
          row.getLong("startTime"), row.getLong("endTime")) }
    }
    Observable.fromIterator(it).handleObservableErrors
  }

}

