package filodb.spark

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import org.velvia.filo.{FastFiloRowReader, FiloVector, RowReader, VectorReader}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import filodb.core._
import filodb.core.query.KeyFilter
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.{RowReaderSegment, ScanMethod, SinglePartitionScan, FilteredPartitionScan}

object FiloRelation extends StrictLogging {
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

  // Parses the Spark filters, matching them to partition key columns, and returning
  // tuples of (position, keytype, applicable filters).
  // NOTE: for now support just one filter per column
  def parsePartitionFilters(projection: RichProjection,
                            filters: Seq[Filter]): Seq[(Int, KeyType, Seq[Filter])] = {
    logger.info(s"Incoming filters = $filters")
    val colToFiltersMap: Map[String, Seq[Filter]] = filters.collect {
      case f @ EqualTo(col, _) => col -> f
      case f @ In(col, _)      => col -> f
    }.groupBy(_._1).mapValues( colFilterPairs => colFilterPairs.map(_._2))

    val columnIdxTypeMap = KeyFilter.mapPartitionColumns(projection, colToFiltersMap.keys.toSeq)

    logger.info(s"Matching partition key col name / pos / keyType: $columnIdxTypeMap")
    columnIdxTypeMap.map { case (colName, (pos, keyType)) =>
      (pos, keyType, colToFiltersMap(colName))
    }.toSeq
  }

  // Turns out Spark 1.4 passes UTF8String's as args a lot. Need to sanitize them.
  def sanitize(item: Any): Any = item match {
    case u: UTF8String => item.toString
    case o: Any        => o
  }

  // Single partition query?
  def singlePartitionQuery(projection: RichProjection,
                           filterStuff: Seq[(Int, KeyType, Seq[Filter])]): Option[Any] = {
    // Are all the partition keys given in filters?
    // Are all the filters EqualTo?
    val equalPredValues: Seq[Any] = filterStuff.collect {
      case (pos, keyType, Seq(EqualTo(_, equalValue))) => sanitize(equalValue)
    }
    if (equalPredValues.length == projection.partitionColumns.length) {
      if (equalPredValues.length == 1) Some(equalPredValues.head) else Some(equalPredValues)
    } else {
      None
    }
  }

  def filtersToFunc(projection: RichProjection,
                    filterStuff: Seq[(Int, KeyType, Seq[Filter])]): Any => Boolean = {
    def toFunc(keyType: KeyType, f: Filter): Any => Boolean = f match {
      case EqualTo(_, value) => KeyFilter.equalsFunc(keyType)(sanitize(value).asInstanceOf[keyType.T])
      case In(_, values)     => KeyFilter.inFunc(keyType)(
                                  values.map(sanitize).toSet.asInstanceOf[Set[keyType.T]])
      case other: Filter     => throw new IllegalArgumentException(s"Sorry, filter $other not supported")
    }

    // Compute one func per column/position
    logger.info(s"Filters by position: $filterStuff")
    val funcs = filterStuff.map { case (pos, keyType, filters) =>
      filters.tail.foldLeft(toFunc(keyType, filters.head)) { case (curFunc, newFilter) =>
        KeyFilter.andFunc(curFunc, toFunc(keyType, newFilter))
      }
    }

    if (funcs.isEmpty) {
      logger.info(s"Using default filtering function")
      (a: Any) => true
    } else {
      KeyFilter.makePartitionFilterFunc(projection, filterStuff.map(_._1), funcs)
    }
  }

  // It's good to put complex functions inside an object, to be sure that everything
  // inside the function does not depend on an explicit outer class and can be serializable
  def perPartitionRowScanner(config: Config,
                             readOnlyProjectionString: String,
                             version: Int,
                             method: ScanMethod): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    FiloSetup.init(config)
    FiloSetup.columnStore    // force startup
    val readOnlyProjection = RichProjection.readOnlyFromString(readOnlyProjectionString)

    parse(FiloSetup.columnStore.scanRows(
            readOnlyProjection, readOnlyProjection.columns, version, method,
            (bytes, clazzes) => new SparkRowReader(bytes, clazzes)),
          10 minutes) { rowIt => rowIt.asInstanceOf[Iterator[Row]] }
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

  def buildScan(): RDD[Row] = buildScan(filoSchema.keys.toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] =
    buildScan(requiredColumns, Array.empty)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Define vars to distribute inside the method
    val _config = this.filoConfig
    val projection = RichProjection(this.datasetObj, filoSchema.values.toSeq)
    val _version = this.version
    val filoColumns = requiredColumns.map(this.filoSchema)
    logger.info(s"Scanning columns ${filoColumns.toSeq}")
    val readOnlyProjStr = projection.toReadOnlyProjString(filoColumns.map(_.name))
    logger.debug(s"readOnlyProjStr = $readOnlyProjStr")

    val parsedFilters = parsePartitionFilters(projection, filters.toList)
    singlePartitionQuery(projection, parsedFilters) match {
      case Some(partitionKey) =>
        sqlContext.sparkContext.parallelize(Seq(partitionKey), 1)
          .mapPartitions { partKeyIter =>
            perPartitionRowScanner(_config, readOnlyProjStr, _version,
                                   SinglePartitionScan(partKeyIter.next))
          }

      case None =>
        val splits = FiloSetup.columnStore.getScanSplits(dataset, splitsPerNode)
        logger.info(s"${splits.length} splits: [${splits.take(3).mkString(", ")}...]")

        val splitsWithLocations = splits.map { s => (s, s.hostnames.toSeq) }

        val filterFunc = filtersToFunc(projection, parsedFilters)

        // NOTE: It's critical that the closure inside mapPartitions only references
        // vars from buildScan() method, and not the FiloRelation class.  Otherwise
        // the entire FiloRelation class would get serialized.
        // Also, each partition should only need one param.
        sqlContext.sparkContext.makeRDD(splitsWithLocations)
          .mapPartitions { splitIter =>
            val _splits = splitIter.toSeq
            require(_splits.length == 1)
            perPartitionRowScanner(_config, readOnlyProjStr, _version,
                                   FilteredPartitionScan(_splits.head, filterFunc))
          }
    }
  }
}

class SparkRowReader(chunks: Array[ByteBuffer],
                     classes: Array[Class[_]],
                     emptyLen: Int = 0)
extends FastFiloRowReader(chunks, classes, emptyLen) with Row {
  def apply(i: Int): Any = getAny(i)
  def copy(): org.apache.spark.sql.Row = {
    val copy = new SparkRowReader(chunks, classes, emptyLen)
    copy.setRowNo(rowNo)
    copy
  }
  def getByte(i: Int): Byte = ???
  def getShort(i: Int): Short = ???
  def isNullAt(i: Int): Boolean = !notNull(i)
  def length: Int = parsers.length
  def toSeq: Seq[Any] =
    (0 until length).map(apply)
}
