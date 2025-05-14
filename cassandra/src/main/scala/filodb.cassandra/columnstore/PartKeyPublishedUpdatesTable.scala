package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.Util.toBuffer
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

class PartKeyPublishedUpdatesTable(val dataset: DatasetRef,
                                   val connector: FiloCassandraConnector,
                                   writeConsistencyLevel: ConsistencyLevel)
                                  (implicit ec: ExecutionContext) extends BaseDatasetTable {

  override def suffix: String = "pks_published_updates"

  override def createCql: String =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableString (
      | shard int,
      | split int,
      | epochHour bigint,
      | startTimeMs bigint,
      | endTimeMs bigint,
      | updatedTimeMs bigint,
      | offset bigint,
      | partKey blob,
      | PRIMARY KEY ((shard, split, epochHour), partKey))
      | WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writeDirtyPartKeyCql = session.prepare(
      s"INSERT INTO $tableString (shard, split, epochHour, startTimeMs, endTimeMs, updatedTimeMs, offset, partKey) " +
        s"VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)
    .setIdempotent(true)

  def writePartKey(split: Int, ttlSeconds: Int, epochHour: Long,
                   updatedTimeMs: Long, offset: Long, pk: PartKeyRecord): Future[Response] = {
    connector.execStmtWithRetries(
      writeDirtyPartKeyCql.bind(
        pk.shard: JInt,
        split: JInt,
        epochHour: JLong,
        pk.startTime: JLong,
        pk.endTime: JLong,
        updatedTimeMs: JLong,
        offset: JLong,
        toBuffer(pk.partKey),
        ttlSeconds: JInt
      )
    )
  }
}
