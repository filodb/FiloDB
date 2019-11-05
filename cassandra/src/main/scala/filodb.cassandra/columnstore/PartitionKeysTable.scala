package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}

case class PartKeyIndexRecord(partKey: ByteBuffer, startTime: Long, endTime: Long)

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
       |) WITH compression = {'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val readCql = s"SELECT segmentid, segment " +
    s"FROM $tableString WHERE shard = ? AND timebucket = ? order by segmentid asc"

  lazy val writePartitionCql =
    session.prepare(
      s"INSERT INTO ${tableString} (partKey, startTime, endTime) VALUES (?, ?, ?) USING TTL ?")
      .setConsistencyLevel(writeConsistencyLevel)

  def writePartKey(shard: Int, partKey: Array[Byte],
                   startTime: Long, endTime: Long, diskTimeToLive: Int): Future[Response] = {
    connector.execStmtWithRetries(writePartitionCql.bind(
      partKey, startTime: JLong, endTime: JLong, diskTimeToLive: JInt))
  }

  def scanPartKeys(tokens: Seq[(String, String)], shard: Int): Observable[PartKeyIndexRecord] = {
    def cql(start: String, end: String): String =
      s"SELECT * FROM ${tableString}_$shard " +
        s"WHERE TOKEN(partKey) >= $start AND TOKEN(partKey) < $end "
    val it = tokens.iterator.flatMap { case (start, end) =>
      session.execute(cql(start, end)).iterator.asScala
        .map { row => PartKeyIndexRecord(row.getBytes("partKey"),
          row.getLong("startTime"), row.getLong("endTime")) }
    }
    Observable.fromIterator(it).handleObservableErrors
  }

}

