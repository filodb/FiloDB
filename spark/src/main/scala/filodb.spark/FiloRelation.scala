package filodb.spark

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.FastFiloRowReader
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.sources.{BaseRelation, TableScan, PrunedScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import filodb.core._
import filodb.core.metadata.{Column, Dataset, DatasetOptions}
import filodb.core.columnstore.RowReaderSegment

object FiloRelation {
  import TypeConverters._

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 5 seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  def actorAsk[B](actor: ActorRef, msg: Any,
                  askTimeout: FiniteDuration = 5 seconds)(f: PartialFunction[Any, B]): B = {
    implicit val timeout = Timeout(askTimeout)
    parse(actor ? msg, askTimeout)(f)
  }

  def getDatasetObj(dataset: String): Dataset =
    parse(FiloSetup.metaStore.getDataset(dataset)) { ds => ds }

  def getSchema(dataset: String, version: Int): Column.Schema =
    parse(FiloSetup.metaStore.getSchema(dataset, version)) { schema => schema }

  def getRows(options: DatasetOptions,
              datasetName: String,
              version: Int,
              columns: Seq[Column],
              sortColumn: Column,
              params: Map[String, String]): Iterator[Row] = {
    val untypedHelper = Dataset.sortKeyHelper(options, sortColumn).get
    implicit val helper = untypedHelper.asInstanceOf[SortKeyHelper[untypedHelper.Key]]
    parse(FiloSetup.columnStore.scanSegments[helper.Key](columns, datasetName, version, params = params),
          10 minutes) { segmentIt =>
      segmentIt.flatMap { seg =>
        val readerSeg = seg.asInstanceOf[RowReaderSegment[helper.Key]]
        readerSeg.rowIterator((bytes, clazzes) => new SparkRowReader(bytes, clazzes))
           .asInstanceOf[Iterator[Row]]
      }
    }
  }

  // It's good to put complex functions inside an object, to be sure that everything
  // inside the function does not depend on an explicit outer class and can be serializable
  def perNodeRowScanner(config: Config,
                        datasetOptionStr: String,
                        version: Int,
                        columns: Seq[Column],
                        sortColumn: Column,
                        paramIter: Iterator[Map[String, String]]): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    FiloSetup.init(config)
    FiloSetup.columnStore    // force startup
    val options = DatasetOptions.fromString(datasetOptionStr)
    val datasetName = sortColumn.dataset

    paramIter.flatMap { param =>
      getRows(options, datasetName, version, columns, sortColumn, param)
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
 * @param sparkContext the spark context to pull config from
 * @param dataset the name of the dataset to read from
 * @param the version of the dataset data to read
 */
case class FiloRelation(dataset: String,
                        version: Int = 0,
                        splitsPerNode: Int = 1)
                       (@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with PrunedScan with StrictLogging {
  import TypeConverters._
  import FiloRelation._

  val filoConfig = FiloSetup.configFromSpark(sqlContext.sparkContext)
  FiloSetup.init(filoConfig)

  val datasetObj = getDatasetObj(dataset)
  val filoSchema = getSchema(dataset, version)

  val schema = StructType(columnsToSqlFields(filoSchema.values.toSeq))
  logger.info(s"Read schema for dataset $dataset = $schema")

  def buildScan(): RDD[Row] = buildScan(filoSchema.keys.toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    // Define vars to distribute inside the method
    val _config = this.filoConfig
    val datasetOptionsStr = this.datasetObj.options.toString
    val _version = this.version
    val filoColumns = requiredColumns.map(this.filoSchema)
    val sortCol = filoSchema(datasetObj.projections.head.sortColumn)
    val splitOpts = Map("splits_per_node" -> splitsPerNode.toString)
    val splits = FiloSetup.columnStore.getScanSplits(dataset, splitOpts)
    logger.info(s"Splits = $splits")

    // NOTE: It's critical that the closure inside mapPartitions only references
    // vars from buildScan() method, and not the FiloRelation class.  Otherwise
    // the entire FiloRelation class would get serialized.
    sqlContext.sparkContext.parallelize(splits, splits.length)
      .mapPartitions { paramIter =>
        perNodeRowScanner(_config, datasetOptionsStr, _version, filoColumns,
                          sortCol, paramIter)
      }
  }
}

class SparkRowReader(chunks: Array[ByteBuffer], classes: Array[Class[_]]) extends
    FastFiloRowReader(chunks, classes) with Row {
  def apply(i: Int): Any = getAny(i)
  def copy(): org.apache.spark.sql.Row = ???
  def getByte(i: Int): Byte = ???
  def getShort(i: Int): Short = ???
  def isNullAt(i: Int): Boolean = !notNull(i)
  def length: Int = parsers.length
  def toSeq: Seq[Any] = ???
}