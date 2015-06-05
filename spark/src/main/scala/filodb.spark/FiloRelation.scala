package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowSetter
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.sources.{BaseRelation, TableScan, PrunedScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import filodb.core.cassandra.CassandraDatastore
import filodb.core.datastore.{Datastore, ReadRowExtractor}
import filodb.core.messages._
import filodb.core.metadata.Column

object FiloRelation {
  val DefaultMinPartitions = 4

  import TypeConverters._

  def parseResponse[B](cmd: => Future[Response])
                      (handler: PartialFunction[Response, B]): B = {
    Await.result(cmd, 5 seconds) match {
      case StorageEngineException(t) => throw new RuntimeException("Error reading from storage", t)
      case e: ErrorResponse =>  throw new RuntimeException("Error: " + e)
      case r: Response => handler(r)
    }
  }

  // It's good to put complex functions inside an object, to be sure that everything
  // inside the function does not depend on an explicit outer class and can be serializable
  def perNodeRowScanner(config: Config, dataset: String, version: Int, columns: Seq[Column],
                        partIter: Iterator[String]): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    val _datastore = new CassandraDatastore(config)
    val system = ActorSystem("FiloRelation")

    partIter.flatMap { partitionName =>
      val partObj = parseResponse(_datastore.getPartition(dataset, partitionName)(system.dispatcher)) {
        case Datastore.ThePartition(partitionObj) => partitionObj
      }
      val rowIter = new ReadRowExtractor(_datastore, partObj, version, columns,
                                         SparkRowSetter)(system)
      val mutRow = new SpecificMutableRow(columnsToSqlTypes(columns))
      new Iterator[Row] {
        def hasNext: Boolean = rowIter.hasNext
        def next: Row = {
          rowIter.next(mutRow)
          mutRow
        }
      }
    }
  }
}

/**
 * Schema and row scanner, with pruned column optimization for fast reading from FiloDB
 *
 * NOTE: Each Spark partition is given 1 to N Filo partitions, and the code sequentially
 * reads data from each partition.  Within each partition read, actors/futures are used to
 * parallelize reads from different columns.
 *
 * @constructor
 * @param filoConfig the Cassandra configuration
 * @param dataset the name of the dataset to read from
 * @param the version of the dataset data to read
 * @param minPartitions the minimum # of partitions to read from
 */
case class FiloRelation(filoConfig: Config,
                        dataset: String,
                        version: Int = 0,
                        minPartitions: Int = FiloRelation.DefaultMinPartitions)
                       (@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with PrunedScan with StrictLogging {
  import TypeConverters._
  import FiloRelation._

  val datastore = new CassandraDatastore(filoConfig)

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  val partitions = parseResponse(datastore.getDataset(dataset)) {
    case Datastore.TheDataset(dataset) =>
      logger.info(s"Got dataset $dataset")
      dataset.partitions
  }

  val filoSchema = parseResponse(datastore.getSchema(dataset, version)) {
    case Datastore.TheSchema(schemaObj) =>
      logger.info(s"Read schema for dataset $dataset = $schemaObj")
      schemaObj
  }

  val schema = StructType(columnsToSqlFields(filoSchema.values.toSeq))

  def buildScan(): RDD[Row] = buildScan(filoSchema.keys.toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // Define vars to distribute inside the method
    val _config = this.filoConfig
    val _dataset = this.dataset
    val _version = this.version
    val filoColumns = requiredColumns.map(this.filoSchema)

    // Right now we are distributing partitions.  Perhaps in the future we should distribute shards.
    // Also one can use makeRDD to send location aware stuff :)
    sqlContext.sparkContext.parallelize(partitions.toSeq, minPartitions)
      .mapPartitions { partIter =>
        perNodeRowScanner(_config, _dataset, _version, filoColumns, partIter)
      }
  }
}

object SparkRowSetter extends RowSetter[MutableRow] {
  def setInt(row: MutableRow, index: Int, data: Int): Unit = row.setInt(index, data)
  def setLong(row: MutableRow, index: Int, data: Long): Unit = row.setLong(index, data)
  def setDouble(row: MutableRow, index: Int, data: Double): Unit = row.setDouble(index, data)
  def setString(row: MutableRow, index: Int, data: String): Unit = row.setString(index, data)

  def setNA(row: MutableRow, index: Int): Unit = row.setNullAt(index)
}