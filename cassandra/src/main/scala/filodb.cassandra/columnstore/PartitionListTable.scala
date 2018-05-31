package filodb.cassandra.columnstore

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, Row}
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response, Types}
import filodb.core.metadata.Dataset

/**
  * This table acts as an index of all the partitions that exist in column store for any given shard.
  *
  * For a given shard there may be millions of partitions, so we store this information in cassandra
  * within multiple stripes. We divide each shard into specific stripes (number of stripes driven by
  * config read by caller)
  *
  * Partition keys are stored against a shard number and a consistently hashed stripe number as well.
  * To fetch all partition keys for a shard, we issue a read for each stripe for the shard.
  */
sealed class PartitionListTable(val dataset: DatasetRef,
                                val connector: FiloCassandraConnector,
                                writeConsistencyLevel: ConsistencyLevel)
                                (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import scala.collection.JavaConverters._

  import filodb.cassandra.Util._

  val suffix = "partitionlist"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    shard int,
                     |    stripe int,
                     |    partition blob,
                     |    PRIMARY KEY ((shard, stripe), partition)
                     |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  def fromRow(row: Row, dataset: Dataset): Types.PartitionKey = ???
    // TODO: in the future we should simply store and retrieve an entire RecordContainer
    // BinaryRecord(dataset.partitionBinSchema, row.getBytes("partition"))

  lazy val readCql = session.prepare(s"SELECT partition FROM $tableString WHERE shard = ? and stripe = ?")
  lazy val writePartitionCql = session.prepare(
    s"INSERT INTO $tableString (shard, stripe, partition) VALUES (?, ?, ?)")
    .setConsistencyLevel(writeConsistencyLevel)

  lazy val deletePartitionCql = session.prepare(
    s"DELETE FROM $tableString WHERE shard = ? and stripe = ? and partition = ?")

  def getPartitions(dataset: Dataset, shard: Int, numStripesPerShard: Int): Observable[Types.PartitionKey] = {
    Observable.range(0, numStripesPerShard).mergeMap { stripe =>
      // mergeMap will parallelize fetching of multiple stripes simultaneously, so it is faster
      val it = session.execute(readCql.bind(Int.box(shard), Int.box(stripe.toInt)))
        .asScala.toIterator.map(fromRow(_, dataset))
      Observable.fromIterator(it).handleObservableErrors
    }
  }

  def writePartitions(shard: Int, stripe: Int, partitions: Seq[Types.PartitionKey]): Future[Response] = {
    val batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
    partitions.foreach { p =>
      // batchStatement.add(writePartitionCql.bind(Int.box(shard), Int.box(stripe), toBuffer(p)))
    }
    connector.execStmtWithRetries(batchStatement)
  }

  def deletePartitions(shard: Int, stripe: Int, partitions: Seq[Types.PartitionKey]): Future[Response] = {
    val batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED)
    partitions.foreach { p =>
      // batchStatement.add(deletePartitionCql.bind(Int.box(shard), Int.box(stripe), toBuffer(p)))
    }
    connector.execStmt(batchStatement)
  }

}