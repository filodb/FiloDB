package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

sealed class PartitionKeysByUpdateTimeTable(val dataset: DatasetRef,
                                            val connector: FiloCassandraConnector,
                                            writeConsistencyLevel: ConsistencyLevel,
                                            readConsistencyLevel: ConsistencyLevel)
                                           (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = s"pks_by_update_time"

  val createCql =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableString (
       |    shard int,
       |    epochHour bigint,
       |    split int,
       |    partKey blob,
       |    startTime bigint,
       |    endTime bigint,
       |    PRIMARY KEY ((shard, epochHour, split), partKey))
       |    WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin
      // TODO time window compaction since we have time component in the primary key

  private lazy val writePartitionKeyCql = session.prepare(
    s"INSERT INTO $tableString (shard, epochHour, split, partKey, startTime, endTime) " +
    s"VALUES (?, ?, ?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)

  private lazy val readCql = session.prepare(
    s"SELECT * FROM $tableString " +
    s"WHERE shard = ? AND epochHour = ? AND split = ? ")
    .setConsistencyLevel(readConsistencyLevel)


  def writePartKey(shard: Int, updateHour: Long, split: Int,
                   pk: PartKeyRecord, ttlSeconds: Int): Future[Response] = {
    connector.execStmtWithRetries(writePartitionKeyCql.bind(
      shard: JInt, updateHour: JLong, split: JInt,
      toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong, ttlSeconds: JInt))
  }

  def scanPartKeys(shard: Int, updateHour: Long, split: Int): Observable[PartKeyRecord] = {
    session.executeAsync(readCql.bind(shard: JInt, updateHour: JLong, split: JInt))
      .toObservable.handleObservableErrors
      .map(PartitionKeysTable.rowToPartKeyRecord)
  }

}

