package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.Util.toBuffer
import filodb.core.{DatasetRef, Response}

class PartitionKeysByUpdateTimeAndOffsetTable(val dataset: DatasetRef,
                                              val connector: FiloCassandraConnector,
                                              writeConsistencyLevel: ConsistencyLevel,
                                              readConsistencyLevel: ConsistencyLevel)
                                             (implicit ec: ExecutionContext) extends BaseDatasetTable {

  override def suffix: String = "pks_by_update_time_offset"

  override def createCql: String =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableString (
      | shard int,
      | startTimeMs bigint,
      | endTimeMs bigint,
      | updatedTimeMs bigint,
      | offset bigint,
      | partKey blob,
      | PRIMARY KEY (partKey, offset)
      | WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writeDirtyPartKeyCql = session.prepare(
      s"INSERT INTO $tableString (shard, startTimeMs, endTimeMs, updatedTimeMs, offset, partKey) " +
        s"VALUES (?, ?, ?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)
    .setIdempotent(true)

  def writePartKey(shard: Int, startTimeMs: Long, endTimeMs: Long, updatedTimeMs: Long, offset: Long,
                   pk: Array[Byte], ttlSeconds: Int): Future[Response] = {
    connector.execStmtWithRetries(
      writeDirtyPartKeyCql.bind(
        shard: JInt,
        startTimeMs: JLong,
        endTimeMs: JLong,
        updatedTimeMs: JLong,
        offset: JLong,
        toBuffer(pk),
        ttlSeconds: JInt
      )
    )
  }
}
