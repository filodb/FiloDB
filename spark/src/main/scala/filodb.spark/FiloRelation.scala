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

import filodb.coordinator.client.Client.parse
import filodb.core._
import filodb.core.query.KeyFilter
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store._

object FiloRelation extends StrictLogging {
  import TypeConverters._

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  def getDatasetObj(dataset: DatasetRef): Dataset =
    parse(FiloDriver.metaStore.getDataset(dataset)) { ds => ds }

  def getSchema(dataset: DatasetRef, version: Int): Column.Schema =
    parse(FiloDriver.metaStore.getSchema(dataset, version)) { schema => schema }

  // Parses the Spark filters, mapping column names to the filters
  def parseFilters(filters: Seq[Filter]): Map[String, Seq[Filter]] = {
    logger.info(s"Incoming filters = $filters")
    filters.collect {
      case f @ EqualTo(col, _) => col -> f
      case f @ In(col, _)      => col -> f
      case f @ GreaterThan(col, _) => col -> f
      case f @ GreaterThanOrEqual(col, _) => col -> f
      case f @ LessThan(col, _) => col -> f
      case f @ LessThanOrEqual(col, _) => col -> f
    }.groupBy(_._1).mapValues( colFilterPairs => colFilterPairs.map(_._2))
  }

  // Parses the Spark filters, matching them to partition key columns, and returning
  // tuples of (position, keytype, applicable filters).
  // NOTE: for now support just one filter per column
  def parsePartitionFilters(projection: RichProjection,
                            groupedFilters: Map[String, Seq[Filter]]): Seq[(Int, KeyType, Seq[Filter])] = {
    val columnIdxTypeMap = KeyFilter.mapPartitionColumns(projection, groupedFilters.keys.toSeq)

    columnIdxTypeMap.map { case (colName, (pos, keyType)) =>
      logger.info(s"Pushing down partition column $colName, filters ${groupedFilters(colName)}")
      (pos, keyType, groupedFilters(colName))
    }.toSeq
  }

  // Turns out Spark 1.4 passes UTF8String's as args a lot. Need to sanitize them.
  def sanitize(item: Any): Any = item match {
    case u: UTF8String => item.toString
    case o: Any        => o
  }

  // Partition Query?
  def partitionQuery(config: Config,
                     projection: RichProjection,
                     filterStuff: Seq[(Int, KeyType, Seq[Filter])]): Seq[Any] = {
    val inqueryPartitionsLimit = config.getInt("columnstore.inquery-partitions-limit")
    // Are all the partition keys given in filters?
    // Are the filters EqualTo, In?
    val predicateValues = filterStuff.collect {
      case (pos, keyType, Seq(EqualTo(_, equalValue))) =>
        Set(KeyFilter.parseSingleValue(keyType)(sanitize(equalValue)))
      case (pos, keyType, Seq(In(_, inValues))) =>
        (KeyFilter.parseValues(keyType)(inValues.map(sanitize).toSet))
    }
    logger.info(s"Push down partition predicates: ${predicateValues.toString()}")
    // 1. Verify if all partition keys are part of the filters or not.
    // 2. If all present then get all possible coombinations of partition keys by calling combine method.
    // 3. If number of partition key combinations are more than inqueryPartitionsLimit then
    // run full table scan otherwise run multipartition scan.
    if (predicateValues.length == projection.partitionColumns.length) {
      if(predicateValues.length == 1) {
        predicateValues.flatten
      } else {
        val predList = combine(predicateValues)
        if (predList.size <= inqueryPartitionsLimit) predList else Nil
      }
    } else {
      Nil
    }
  }

  /**
    * Method to get possible combinations of key values provided in partition key filters.
    * For example filters provided in the query are actor2Code IN ('JPN', 'KHM') and year = 1979
    * then this method returns all combinations like ('JPN',1979) , ('KHM',1979) which represents
    * full partition key value.
    */
  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.filter(_.nonEmpty).foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for {a <- x; b <- y} yield a :+ b
    }

  def segmentRangeScan(projection: RichProjection,
                       groupedFilters: Map[String, Seq[Filter]]): Option[SegmentRange[projection.SK]] = {
    val columnIdxTypeMap = KeyFilter.mapSegmentColumn(projection, groupedFilters.keys.toSeq)
    if (columnIdxTypeMap.isEmpty) { None }
    else {
      val (colName, (_, keyType)) = columnIdxTypeMap.head
      val filters = groupedFilters(colName)
      logger.info(s"Incoming Segment Filters [$filters]")
      if (filters.length == 1) {
        // Filters with segment column name (or source column) matches, and ensure we have = filter
        filters match {
          case Seq(EqualTo(_, filterValue)) =>
            val equalVal = KeyFilter.parseSingleValue(keyType)(filterValue)
            logger.info(s"Pushing down segment range [$equalVal,$equalVal] on column $colName...")
            Some(SegmentRange(equalVal, equalVal).basedOn(projection))
          case other: Any => None
        }
      } else if (filters.length == 2) {
        // Filters with segment column name (or source column) matches, and ensure we have >/>= and </<= filters
        val leftRightValues = filters match {
          case Seq(GreaterThan(_, lVal),        LessThan(_, rVal)) => Some((lVal, rVal))
          case Seq(GreaterThanOrEqual(_, lVal), LessThan(_, rVal)) => Some((lVal, rVal))
          case Seq(GreaterThan(_, lVal),        LessThanOrEqual(_, rVal)) => Some((lVal, rVal))
          case Seq(GreaterThanOrEqual(_, lVal), LessThanOrEqual(_, rVal)) => Some((lVal, rVal))
          case other: Any => None
        }
        leftRightValues match {
          case Some((lVal, rVal)) =>
            val leftValue = KeyFilter.parseSingleValue(keyType)(lVal)
            val rightValue = KeyFilter.parseSingleValue(keyType)(rVal)
            logger.info(s"Pushing down segment range [$leftValue, $rightValue] on column $colName...")
            Some(SegmentRange(leftValue, rightValue).basedOn(projection))
          case None =>
            logger.info(s"Segment column $colName matches, but cannot push down filters $filters")
            None
        }
      } else{
        logger.info(s"Segment column $colName matches, but cannot push down with ${filters.length} filters")
        None
      }
    }
  }

  def filtersToFunc(projection: RichProjection,
                    filterStuff: Seq[(Int, KeyType, Seq[Filter])]): Any => Boolean = {
    import KeyFilter._

    def toFunc(keyType: KeyType, f: Filter): Any => Boolean = f match {
      case EqualTo(_, value) => equalsFunc(keyType)(parseSingleValue(keyType)(sanitize(value)))
      case In(_, values)     => inFunc(keyType)(parseValues(keyType)(values.map(sanitize).toSet).toSet)
      case other: Filter     => throw new IllegalArgumentException(s"Sorry, filter $other not supported")
    }

    // Compute one func per column/position
    logger.debug(s"Filters by position: $filterStuff")
    val funcs = filterStuff.map { case (pos, keyType, filters) =>
      filters.tail.foldLeft(toFunc(keyType, filters.head)) { case (curFunc, newFilter) =>
        andFunc(curFunc, toFunc(keyType, newFilter))
      }
    }

    if (funcs.isEmpty) {
      logger.info(s"Scanning all partitions with no partition key filtering")
      (a: Any) => true
    } else {
      makePartitionFilterFunc(projection, filterStuff.map(_._1), funcs)
    }
  }

  def splitsQuery(sqlContext: SQLContext,
                  dataset: DatasetRef,
                  splitsPerNode: Int,
                  config: Config,
                  readOnlyProjStr: String,
                  version: Int)(f: ScanSplit => ScanMethod): RDD[Row] = {
    val splits = FiloDriver.columnStore.getScanSplits(dataset, splitsPerNode)
    logger.info(s"${splits.length} splits: [${splits.take(3).mkString(", ")}...]")

    val splitsWithLocations = splits.map { s => (s, s.hostnames.toSeq) }
    // NOTE: It's critical that the closure inside mapPartitions only references
    // vars from buildScan() method, and not the FiloRelation class.  Otherwise
    // the entire FiloRelation class would get serialized.
    // Also, each partition should only need one param.
    sqlContext.sparkContext.makeRDD(splitsWithLocations)
      .mapPartitions { splitIter =>
        perPartitionRowScanner(config, readOnlyProjStr, version, f(splitIter.next))
      }
  }

  // It's good to put complex functions inside an object, to be sure that everything
  // inside the function does not depend on an explicit outer class and can be serializable
  def perPartitionRowScanner(config: Config,
                             readOnlyProjectionString: String,
                             version: Int,
                             method: ScanMethod): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    FiloExecutor.init(config)
    FiloExecutor.columnStore    // force startup
    val readOnlyProjection = RichProjection.readOnlyFromString(readOnlyProjectionString)

    parse(FiloExecutor.columnStore.scanRows(
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
 * @param dataset the DatasetRef with name and database/keyspace of the dataset to read from
 * @param the version of the dataset data to read
 */
case class FiloRelation(dataset: DatasetRef,
                        version: Int = 0,
                        splitsPerNode: Int = 1)
                       (@transient val sqlContext: SQLContext)
    extends BaseRelation with InsertableRelation with PrunedScan with PrunedFilteredScan with StrictLogging {
  import TypeConverters._
  import FiloRelation._

  val filoConfig = FiloDriver.initAndGetConfig(sqlContext.sparkContext)

  val datasetObj = getDatasetObj(dataset)
  val filoSchema = getSchema(dataset, version)

  val schema = StructType(columnsToSqlFields(filoSchema.values.toSeq))
  logger.info(s"Read schema for dataset $dataset = $schema")

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    sqlContext.insertIntoFilo(data, dataset.dataset, version, overwrite, dataset.database)

  def buildScan(): RDD[Row] = buildScan(filoSchema.keys.toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] =
    buildScan(requiredColumns, Array.empty)

  def scanByPartitions(projection: RichProjection,
                       groupedFilters: Map[String, Seq[Filter]],
                       partitionFilters: Seq[(Int, KeyType, Seq[Filter])],
                       readOnlyProjStr: String): RDD[Row] = {
    val _config = this.filoConfig
    val _version = this.version
    val segmentRange = segmentRangeScan(projection, groupedFilters)
    (partitionQuery(_config,projection, partitionFilters), segmentRange) match {
      // single partition query, no range scan
      case (Seq(partitionKey), None) =>
        sqlContext.sparkContext.parallelize(Seq(partitionKey), 1).mapPartitions { partKeyIter =>
          perPartitionRowScanner(_config, readOnlyProjStr, _version,
            SinglePartitionScan(partKeyIter.next))
        }
      // single partition query, with range scan
      case (Seq(partitionKey), Some(segRange)) =>
        val keyRange = KeyRange(partitionKey, segRange.start, segRange.end, endExclusive = false)
        sqlContext.sparkContext.parallelize(Seq(keyRange), 1).mapPartitions { keyRangeIter =>
          perPartitionRowScanner(_config, readOnlyProjStr, _version,
            SinglePartitionRangeScan(keyRangeIter.next))
        }

      // filtered partition full table scan
      case (Nil, None) =>
        val filterFunc = filtersToFunc(projection, partitionFilters)
        splitsQuery(sqlContext, dataset, splitsPerNode, _config, readOnlyProjStr, _version) { s =>
          FilteredPartitionScan(s, filterFunc)
        }
      // filtered partition query, with range scan
      case (Nil, Some(segRange)) =>
        val filterFunc = filtersToFunc(projection, partitionFilters)
        splitsQuery(sqlContext, dataset, splitsPerNode, _config, readOnlyProjStr, _version) { s =>
          FilteredPartitionRangeScan(s, segRange, filterFunc)
        }

      // multi partition query, no range scan
      case (partitionKeys, None) =>
        sqlContext.sparkContext.parallelize(partitionKeys, 1).mapPartitions { partKeyIter =>
          perPartitionRowScanner(_config, readOnlyProjStr, _version,
            MultiPartitionScan(partKeyIter.toSeq))
        }
      // multi partition query, with range scan
      case (partitionKeys, Some(segRange)) =>
        val keyRanges = partitionKeys.map { partitionKey =>
          KeyRange(partitionKey, segRange.start, segRange.end, endExclusive = false) }
        sqlContext.sparkContext.parallelize(keyRanges, 1).mapPartitions { keyRangeIter =>
          perPartitionRowScanner(_config, readOnlyProjStr, _version,
            MultiPartitionRangeScan(keyRangeIter.toSeq))
        }
    }
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Define vars to distribute inside the method
    val projection = RichProjection(this.datasetObj, filoSchema.values.toSeq)
    val filoColumns = requiredColumns.map(this.filoSchema)
    logger.info(s"Scanning columns ${filoColumns.toSeq}")
    val readOnlyProjStr = projection.toReadOnlyProjString(filoColumns.map(_.name))
    logger.debug(s"readOnlyProjStr = $readOnlyProjStr")

    val groupedFilters = parseFilters(filters.toList)
    val partitionFilters = parsePartitionFilters(projection, groupedFilters)

    scanByPartitions(projection, groupedFilters, partitionFilters, readOnlyProjStr)
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
