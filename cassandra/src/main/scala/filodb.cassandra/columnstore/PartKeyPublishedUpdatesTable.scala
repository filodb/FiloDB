package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

sealed class PartKeyPublishedUpdatesTable(val dataset: DatasetRef,
                                   val connector: FiloCassandraConnector,
                                   writeConsistencyLevel: ConsistencyLevel,
                                   readConsistencyLevel: ConsistencyLevel)
                                  (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  override def suffix: String = "pks_published_updates"

  override def createCql: String =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableString (
      | shard int,
      | split int,
      | epoch5mBucket bigint,
      | startTimeMs bigint,
      | endTimeMs bigint,
      | updatedTimeMs bigint,
      | offset bigint,
      | partKey blob,
      | PRIMARY KEY ((shard, split, epoch5mBucket), partKey))
      | WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writeDirtyPartKeyCql = session.prepare(
    s"INSERT INTO $tableString (shard, split, epoch5mBucket, startTimeMs, endTimeMs, updatedTimeMs, offset, partKey) " +
    s"VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)
    .setIdempotent(true)

  private lazy val readCql = session.prepare(
      s"SELECT * FROM $tableString " +
        s"WHERE shard = ? AND epoch5mBucket = ? AND split = ? ")
    .setConsistencyLevel(readConsistencyLevel)
    .setIdempotent(true)

  def writePartKey(split: Int, ttlSeconds: Int, epoch5mBucket: Long,
                   updatedTimeMs: Long, offset: Long, pk: PartKeyRecord): Future[Response] = {
    connector.execStmtWithRetries(
      writeDirtyPartKeyCql.bind(
        pk.shard: JInt,
        split: JInt,
        epoch5mBucket: JLong,
        pk.startTime: JLong,
        pk.endTime: JLong,
        updatedTimeMs: JLong,
        offset: JLong,
        toBuffer(pk.partKey),
        ttlSeconds: JInt
      )
    )
  }

  def scanPartKeys(shard: Int, timeBucket: Long, split: Int): Observable[PartKeyRecord] = {
    session.executeAsync(readCql.bind(shard: JInt, timeBucket: JLong, split: JInt))
      .toObservable.handleObservableErrors
      .map(r => PartitionKeysTable.updatesRowToPartKeyRecord(r, shard))
  }
}
