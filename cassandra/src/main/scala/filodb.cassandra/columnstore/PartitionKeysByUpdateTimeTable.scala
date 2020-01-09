package filodb.cassandra.columnstore

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyRecord

sealed class PartitionKeysByUpdateTimeTable(val dataset: DatasetRef,
                                            val connector: FiloCassandraConnector)
                                           (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = s"_partitionKeysByUpdateTime"

  val createCql =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableString (
       |    shard int,
       |    epochHour bigint,
       |    split int,
       |    partKey blob,
       |    startTime bigint,
       |    endTime bigint,
       |    PRIMARY KEY ((shard, epochHour, split), partKey)
       |    WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin
      // TODO time window compaction since we have time component in the primary key

  lazy val writePartitionKeyCql =
    session.prepare(
      s"INSERT INTO ${tableString} (shard, epochHour, split, partKey, startTime, endTime) " +
        s"VALUES (?, ?, ?) USING TTL ?")
      .setConsistencyLevel(ConsistencyLevel.ONE)

  def writePartKey(shard: Int, updateHour: Long, split: Int, pk: PartKeyRecord): Future[Response] = {
    connector.execStmtWithRetries(writePartitionKeyCql.bind(
      shard: JInt, updateHour: JLong,
      toBuffer(pk.partKey), pk.startTime: JLong, pk.endTime: JLong))
  }

  def scanPartKeys(shard: Int, updateHour: Long, split: Int): Observable[PartKeyRecord] = {
    val cql = s"SELECT * FROM ${tableString} " +
              s"WHERE shard = $shard AND updateHour = $updateHour AND split = $split "
    val it = session.execute(cql).iterator.asScala
        .map(PartitionKeysTable.rowToPartKeyRecord)
    Observable.fromIterator(it).handleObservableErrors
  }

}

