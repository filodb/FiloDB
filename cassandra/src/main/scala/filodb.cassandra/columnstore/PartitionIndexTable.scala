package filodb.cassandra.columnstore

import java.lang.{Integer => JInt}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.Util.unloggedBatch
import filodb.core.{DatasetRef, Response}
import filodb.core.store.PartKeyTimeBucketSegment

/**
  * This table acts as an index of all the partition index that exist in column store for any given shard.
  *
  * For a given shard there may be millions of partition indexes, so we store this information in cassandra
  * within multiple stripes. We divide each shard into specific stripes (number of stripes driven by
  * config read by caller)
  *
  * Partition Indexes are stored against a shard number and a consistently hashed stripe number as well.
  * To fetch all partition keys for a shard, we issue a read for each stripe for the shard.
  */
sealed class PartitionIndexTable(val dataset: DatasetRef,
                                 val connector: FiloCassandraConnector,
                                 writeConsistencyLevel: ConsistencyLevel)
                                (implicit ec: ExecutionContext) extends BaseDatasetTable {

  val suffix = "partitionindex"

  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    shard int,
       |    timebucket int,
       |    segmentid int,
       |    segment blob,
       |    PRIMARY KEY ((shard, timebucket), segmentid)
       |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val readCql = session.prepare(s"SELECT segmentid, segment " +
    s"FROM $tableString WHERE shard = ? AND timebucket = ? order by segmentid asc")

  lazy val writePartitionCql = session.prepare(
    s"INSERT INTO $tableString (shard, timebucket, segmentid, segment) VALUES (?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)

  def getPartKeySegments(shard: Int, timeBucket: Int): Observable[PartKeyTimeBucketSegment] = {
    val it = session.execute(readCql.bind(shard: JInt, timeBucket: JInt))
      .asScala.toIterator.map(row => {
      PartKeyTimeBucketSegment(row.getInt("segmentid"),  row.getBytes("segment"))
    })
    Observable.fromIterator(it)
  }

  def writePartKeySegments(shard: Int, timeBucket: Int,
                          segments: Seq[ByteBuffer], diskTimeToLive: Int): Future[Response] = {
    val statements = segments.zipWithIndex.map { case (segment, segmentId) =>
      writePartitionCql.bind(shard: JInt, timeBucket: JInt, segmentId: JInt, segment, diskTimeToLive: JInt)
    }
    connector.execStmtWithRetries(unloggedBatch(statements).setConsistencyLevel(writeConsistencyLevel))
  }

}

