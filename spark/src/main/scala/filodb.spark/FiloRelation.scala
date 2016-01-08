package filodb.spark

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import org.velvia.filo.{FiloRowReader, FiloVector, ParsersFromChunks}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, GenericInternalRow}
import org.apache.spark.sql.sources.{BaseRelation, TableScan, PrunedScan, PrunedFilteredScan, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import filodb.core._
import filodb.core.metadata.{Column, Dataset, DatasetOptions}
import filodb.core.store.RowReaderSegment

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
              filterFunc: Types.PartitionKey => Boolean,
              params: Map[String, String]): Iterator[Row] = {
    val untypedHelper = Dataset.sortKeyHelper(options, sortColumn).get
    implicit val helper = untypedHelper.asInstanceOf[SortKeyHelper[untypedHelper.Key]]
    parse(FiloSetup.columnStore.scanSegments[helper.Key](columns, datasetName, version,
                                                         filterFunc, params = params),
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
  def perPartitionRowScanner(config: Config,
                        datasetOptionStr: String,
                        version: Int,
                        columns: Seq[Column],
                        sortColumn: Column,
                        filterFunc: Types.PartitionKey => Boolean,
                        param: Map[String, String]): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    FiloSetup.init(config)
    FiloSetup.columnStore    // force startup
    val options = DatasetOptions.fromString(datasetOptionStr)
    val datasetName = sortColumn.dataset

    getRows(options, datasetName, version, columns, sortColumn, filterFunc, param)
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
    extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with StrictLogging {
  import TypeConverters._
  import FiloRelation._

  val filoConfig = FiloSetup.configFromSpark(sqlContext.sparkContext)
  FiloSetup.init(filoConfig)

  val datasetObj = getDatasetObj(dataset)
  val filoSchema = getSchema(dataset, version)

  val schema = StructType(columnsToSqlFields(filoSchema.values.toSeq))
  logger.info(s"Read schema for dataset $dataset = $schema")

  // Return false when returning RDD[InternalRow]
  override def needConversion: Boolean = false

  def buildScan(): RDD[Row] = buildScan(filoSchema.keys.toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] =
    buildScan(requiredColumns, Array.empty)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Define vars to distribute inside the method
    val _config = this.filoConfig
    val datasetOptionsStr = this.datasetObj.options.toString
    val _version = this.version
    val filoColumns = requiredColumns.map(this.filoSchema)
    val sortCol = filoSchema(datasetObj.projections.head.sortColumn)
    val splitOpts = Map("splits_per_node" -> splitsPerNode.toString)
    val splits = FiloSetup.columnStore.getScanSplits(dataset, splitOpts)
    logger.info(s"Splits = $splits")

    val filterFuncs = getFilterFuncs(filters.toList)
    require(filterFuncs.length <= 1, "More than one filtering function not supported right now")
    val filterFunc = if (filterFuncs.isEmpty) ((p: Types.PartitionKey) => true) else filterFuncs.head

    // NOTE: It's critical that the closure inside mapPartitions only references
    // vars from buildScan() method, and not the FiloRelation class.  Otherwise
    // the entire FiloRelation class would get serialized.
    // Also, each partition should only need one param.
    sqlContext.sparkContext.parallelize(splits, splits.length)
      .mapPartitions { paramIter =>
        val params = paramIter.toSeq
        require(params.length == 1)
        perPartitionRowScanner(_config, datasetOptionsStr, _version, filoColumns,
                               sortCol, filterFunc, params.head)
      }
  }

  // For now just implement really simple filtering
  private def getFilterFuncs(filters: Seq[Filter]): Seq[Types.PartitionKey => Boolean] = {
    import org.apache.spark.sql.sources._
    logger.info(s"Filters = $filters")
    filters.flatMap {
      case EqualTo(datasetObj.partitionColumn, equalVal) =>
        val compareVal = equalVal.toString   // must convert to same type as partitionKey
        logger.info(s"Adding filter predicate === $compareVal")
        Some((p: Types.PartitionKey) => p == compareVal)
      case other: Filter =>
        None
    }
  }
}

/**
 * Base the SparkRowReader on Spark's InternalRow.  Scans are much faster because
 * sources that return RDD[Row] need to be converted to RDD[InternalRow], and the
 * conversion is horribly expensive, especially for primitives.
 * The only downside is that the API is not stable.  :/
 * TODO: get UTF8String's out of Filo as well, so no string conversion needed :D
 */
class SparkRowReader(val chunks: Array[ByteBuffer],
                     val classes: Array[Class[_]],
                     val emptyLen: Int = 0)
extends BaseGenericInternalRow with FiloRowReader with ParsersFromChunks {
  var rowNo: Int = -1
  final def setRowNo(newRowNo: Int): Unit = { rowNo = newRowNo }

  final def notNull(columnNo: Int): Boolean = parsers(columnNo).isAvailable(rowNo)

  override final def getBoolean(columnNo: Int): Boolean =
    parsers(columnNo).asInstanceOf[FiloVector[Boolean]](rowNo)
  override final def getInt(columnNo: Int): Int =
    parsers(columnNo).asInstanceOf[FiloVector[Int]](rowNo)
  override final def getLong(columnNo: Int): Long =
    parsers(columnNo).asInstanceOf[FiloVector[Long]](rowNo)
  override final def getDouble(columnNo: Int): Double =
    parsers(columnNo).asInstanceOf[FiloVector[Double]](rowNo)
  override final def getFloat(columnNo: Int): Float =
    parsers(columnNo).asInstanceOf[FiloVector[Float]](rowNo)

  // NOTE: This is horribly slow as it decodes a String from Filo's UTF8, then has
  // to convert back to string.  When Filo gets UTF8 support then use
  // UTF8String.fromAddress(object, offset, numBytes)... might enable zero copy strings
  override final def getUTF8String(columnNo: Int): UTF8String = {
    val str = parsers(columnNo).asInstanceOf[FiloVector[String]](rowNo)
    UTF8String.fromString(str)
  }

  final def getDateTime(columnNo: Int): DateTime = parsers(columnNo).asInstanceOf[FiloVector[DateTime]](rowNo)

  final def genericGet(columnNo: Int): Any = parsers(columnNo).boxed(rowNo)
  override final def isNullAt(i: Int): Boolean = !notNull(i)
  def numFields: Int = parsers.length

  // Only used for pure select() queries, doesn't need to be fast?
  def copy(): InternalRow =
    new GenericInternalRow((0 until numFields).map(genericGet).toArray)
}
