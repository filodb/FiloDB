package filodb.spark

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, GenericInternalRow}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import filodb.coordinator.client.Client.parse
import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query.{ColumnFilter, Filter => FF, KeyFilter}
import filodb.core.store._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}

object FiloRelation extends StrictLogging {
  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  // *** Statistics **
  // For now they are global across all tables.
  // TODO(velvia): Make them per-table?
  val totalQueries             = Kamon.counter("spark-queries-total")
  val singlePartQueries        = Kamon.counter("spark-queries-single-partition")
  val multiPartQueries         = Kamon.counter("spark-queries-multi-partition")
  val fullFilteredQueries      = Kamon.counter("spark-queries-full-filtered")
  val fullTableQueries         = Kamon.counter("spark-queries-full-table")

  def getDatasetObj(dataset: DatasetRef): Dataset =
    parse(FiloDriver.metaStore.getDataset(dataset)) { ds => ds }

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
  // tuples of (position, column, applicable filters).
  // NOTE: for now support just one filter per column
  def parsePartitionFilters(dataset: Dataset,
                            groupedFilters: Map[String, Seq[Filter]]): Seq[(String, Column, Seq[Filter])] = {
    val columnIdxTypeMap = KeyFilter.mapPartitionColumns(dataset, groupedFilters.keys.toSeq)

    columnIdxTypeMap.map { case (colName, (pos, column)) =>
      logger.info(s"Pushing down partition column $colName, filters ${groupedFilters(colName)}")
      (colName, column, groupedFilters(colName))
    }.toSeq
  }

  // Partition Query?
  def partitionQuery(config: Config,
                     dataset: Dataset,
                     filterStuff: Seq[(String, Column, Seq[Filter])]): Seq[Array[Byte]] = {
    val inqueryPartitionsLimit = config.getInt("columnstore.inquery-partitions-limit")
    // Are all the partition keys given in filters?
    // Are the filters EqualTo, In?
    val predicateValues = filterStuff.collect {
      case (_, column, Seq(EqualTo(_, equalValue))) =>
        Set(conv(equalValue))
      case (_, column, Seq(In(_, inValues))) =>
        inValues.map(conv).toSet
    }
    logger.info(s"Push down partition predicates: ${predicateValues.toString()}")
    // 1. Verify if all partition keys are part of the filters or not.
    // 2. If all present then get all possible coombinations of partition keys by calling combine method.
    // 3. If number of partition key combinations are more than inqueryPartitionsLimit then
    // run full table scan otherwise run multipartition scan.
    if (predicateValues.length == dataset.partitionColumns.length) {
      val partKeys = combine(predicateValues).map { values => dataset.partKey(values: _*) }
      if (partKeys.size <= inqueryPartitionsLimit) partKeys else Nil
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

  // PFs for evaluating range scans
  val equalsRangePF: PartialFunction[(Column, Seq[Filter]), Option[(Any, Any)]] = {
    case (_, Seq(EqualTo(_, filterValue))) => Some((filterValue, filterValue))
  }

  val betweenRangePF: PartialFunction[(Column, Seq[Filter]), Option[(Any, Any)]] = {
    case (_, Seq(GreaterThan(_, lVal),        LessThan(_, rVal))) => Some((lVal, rVal))
    case (_, Seq(GreaterThanOrEqual(_, lVal), LessThan(_, rVal))) => Some((lVal, rVal))
    case (_, Seq(GreaterThan(_, lVal),        LessThanOrEqual(_, rVal))) => Some((lVal, rVal))
    case (_, Seq(GreaterThanOrEqual(_, lVal), LessThanOrEqual(_, rVal))) => Some((lVal, rVal))
  }

  val defaultRangePF: PartialFunction[(Column, Seq[Filter]), Option[(Any, Any)]] = {
    case other: Any                        => None
  }

  val nonLastPartPF = (equalsRangePF orElse defaultRangePF)
  val lastPartPF = (equalsRangePF orElse betweenRangePF orElse defaultRangePF)

  /**
   * Evaluate if we can do range scans within a partition's chunks given the filters from Spark.
   */
  def chunkRangeScan(dataset: Dataset,
                     groupedFilters: Map[String, Seq[Filter]]): ChunkScanMethod = {
    val columnIdxTypeMap = KeyFilter.mapRowKeyColumns(dataset, groupedFilters.keys.toSeq)

    val posTypeFilters = columnIdxTypeMap.map { case (colName, (pos, col)) =>
      logger.debug(s"For row key column $colName, filters are ${groupedFilters(colName)}")
      pos -> (col -> groupedFilters(colName))
    }

    val sortedPositions = posTypeFilters.keys.toBuffer.sorted

    if (columnIdxTypeMap.isEmpty) { AllChunkScan }
    // Step 1: Only valid filters are on columns from 0 to n where n < # row keys
    else if (sortedPositions.last == sortedPositions.length - 1) {
      // Step 2: The valid range filters are:
      //   (=)
      //   (><)
      //   (=, ><)
      //   (=, =)
      //   (=, =, ><)  etc.
      //   IE the last component must be either = or >< and all others must be =
      val firstRanges = (sortedPositions dropRight 1).map { pos => nonLastPartPF(posTypeFilters(pos)) }
      val ranges = firstRanges :+ lastPartPF(posTypeFilters(sortedPositions.last))
      val invalidPositions = ranges.zipWithIndex.collect { case (None, idx) => idx }
      if (invalidPositions.nonEmpty) {
        invalidPositions.foreach { pos =>
          logger.info(s"Filters ${posTypeFilters(pos)._2} cannot be pushed down for rowkey range scans")
        }
        AllChunkScan
      } else {
        // WARNING: we take the first value from the keys and assume it is the timestamp.
        val startTime = ranges.head.get._1.asInstanceOf[Long]
        val endTime = ranges.head.get._2.asInstanceOf[Long]
        logger.info(s"Pushdown of time range ($startTime, $endTime)")
        TimeRangeChunkScan(startTime, endTime)
      }
    } else {
      logger.info(s"Filters $groupedFilters skipped some row key columns, must be from row key columns 0..n")
      logger.info(s"...but are in positions $sortedPositions")
      AllChunkScan
    }
  }

  // Convert values if needed, in particular strings
  private def conv(value: Any): Any = value match {
    case s: String     => ZeroCopyUTF8String(s)
    case u: UTF8String => new ZeroCopyUTF8String(u.getBaseObject, u.getBaseOffset, u.numBytes)
    case o: Any        => o
  }

  // Returns ColumnFilters for partition column filters
  def getPartitionFilters(dataset: Dataset,
                          partFilters: Seq[(String, Column, Seq[Filter])]): Seq[ColumnFilter] = {
    import KeyFilter._

    def toFilter(col: Column, f: Filter): FF = f match {
      case EqualTo(_, value) => FF.Equals(parseSingleValue(col, conv(value)))
      case In(_, values)     => FF.In(parseValues(col, values.map(conv)).toSet)
      case other: Filter     => throw new IllegalArgumentException(s"Sorry, filter $other not supported")
    }

    // Compute one func per column/position
    logger.debug(s"Filters by position: $partFilters")
    val colFilters = partFilters.map { case (name, col, filters) =>
      val filoFilter = filters.tail.foldLeft(toFilter(col, filters.head)) { case (curFilter, newFilter) =>
        FF.And(curFilter, toFilter(col, newFilter))
      }
      ColumnFilter(name, filoFilter)
    }

    if (colFilters.isEmpty) logger.info(s"Scanning all partitions with no partition key filtering")
    colFilters
  }

  def splitsQuery(sqlContext: SQLContext,
                  dataset: DatasetRef,
                  splitsPerNode: Int,
                  confStr: String,
                  serializedDataset: String,
                  columnIDs: Seq[Int],
                  chunkMethod: ChunkScanMethod)(f: ScanSplit => PartitionScanMethod): RDD[Row] = {
    val splits = FiloDriver.memStore.getScanSplits(dataset, splitsPerNode)
    logger.info(s"${splits.length} splits: [${splits.take(3).mkString(", ")}...]")

    val splitsWithLocations = splits.map { s => (s, s.hostnames.toSeq) }
    // NOTE: It's critical that the closure inside mapPartitions only references
    // vars from buildScan() method, and not the FiloRelation class.  Otherwise
    // the entire FiloRelation class would get serialized.
    // Also, each partition should only need one param.
    sqlContext.sparkContext.makeRDD(splitsWithLocations)
      .mapPartitions { splitIter =>
        perPartitionRowScanner(confStr, serializedDataset, columnIDs, f(splitIter.next), chunkMethod)
      }
  }

  // It's good to put complex functions inside an object, to be sure that everything
  // inside the function does not depend on an explicit outer class and can be serializable
  def perPartitionRowScanner(confStr: String,
                             serializedDataset: String,
                             columnIDs: Seq[Int],
                             partMethod: PartitionScanMethod,
                             chunkMethod: ChunkScanMethod): Iterator[Row] = {
    // NOTE: all the code inside here runs distributed on each node.  So, create my own datastore, etc.
    FiloExecutor.init(confStr)
    FiloExecutor.memStore    // force startup
    val dataset = Dataset.fromCompactString(serializedDataset)

    val sparkReader = new SparkRowReader(dataset, columnIDs)
    FiloExecutor.memStore.scanRows(dataset, columnIDs, partMethod, chunkMethod)
                .map { filoRow =>
                  sparkReader.filoRowReader = filoRow
                  sparkReader.asInstanceOf[Row]
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
 * @param dataset the DatasetRef with name and database/keyspace of the dataset to read from
 */
case class FiloRelation(dataset: DatasetRef,
                        splitsPerNode: Int = 1)
                       (@transient val sqlContext: SQLContext)
    extends BaseRelation with InsertableRelation with PrunedScan with PrunedFilteredScan with StrictLogging {
  import FiloRelation._
  import TypeConverters._

  val filoConfig = FiloDriver.initAndGetConfig(sqlContext.sparkContext)

  val datasetObj = getDatasetObj(dataset)
  val allColumns = datasetObj.partitionColumns ++ datasetObj.dataColumns

  val schema = StructType(columnsToSqlFields(allColumns))
  logger.info(s"Read schema for dataset $dataset = $schema")

  // Return false when returning RDD[InternalRow]
  override def needConversion: Boolean = false

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    sqlContext.insertIntoFilo(data, dataset.dataset, overwrite, dataset.database)

  def buildScan(): RDD[Row] = buildScan(allColumns.map(_.name).toArray)

  def buildScan(requiredColumns: Array[String]): RDD[Row] =
    buildScan(requiredColumns, Array.empty)

  def parallelizePartKeys(keys: Seq[Array[Byte]], nPartitions: Int): RDD[Array[Byte]] =
    sqlContext.sparkContext.parallelize(keys, nPartitions)

  def scanByPartitions(dataset: Dataset,
                       groupedFilters: Map[String, Seq[Filter]],
                       partitionFilters: Seq[(String, Column, Seq[Filter])],
                       columnIDs: Seq[Int]): RDD[Row] = {
    val _config = this.filoConfig
    val _confStr = _config.root.render(ConfigRenderOptions.concise)
    val _serializedDataset = dataset.asCompactString
    totalQueries.increment
    val chunkMethod = chunkRangeScan(dataset, groupedFilters)
    partitionQuery(_config, dataset, partitionFilters) match {
      // single partition query
      case Seq(partitionKey) =>
        singlePartQueries.increment
        parallelizePartKeys(Seq(partitionKey), 1).mapPartitions { partKeyIter =>
          perPartitionRowScanner(_confStr, _serializedDataset, columnIDs,
                                 SinglePartitionScan(partKeyIter.next), chunkMethod)
        }

      // filtered partition full table scan, with or without range scanning
      case Nil =>
        val colFilters = getPartitionFilters(dataset, partitionFilters)
        if (colFilters.nonEmpty) fullFilteredQueries.increment else fullTableQueries.increment
        splitsQuery(sqlContext, dataset.ref, splitsPerNode, _confStr, _serializedDataset, columnIDs, chunkMethod) { s =>
          FilteredPartitionScan(s, colFilters)
        }

      // multi partition query, no range scan
      case partitionKeys: Seq[Array[Byte]] =>
        multiPartQueries.increment
        parallelizePartKeys(partitionKeys, 1).mapPartitions { partKeyIter =>
          perPartitionRowScanner(_confStr, _serializedDataset, columnIDs,
                                 MultiPartitionScan(partKeyIter.toSeq), chunkMethod)
        }
    }
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val columnIDs =  // for select count(*), read a partition column just to get row counts
      if (requiredColumns.isEmpty) { Seq(datasetObj.partitionColumns.head.id) }
      else { datasetObj.colIDs(requiredColumns.toSeq: _*).get }
    logger.info(s"Scanning columns $columnIDs")

    val groupedFilters = parseFilters(filters.toList)
    val partitionFilters = parsePartitionFilters(datasetObj, groupedFilters)

    scanByPartitions(datasetObj, groupedFilters, partitionFilters, columnIDs)
  }
}

/**
 * Base the SparkRowReader on Spark's InternalRow.  Scans are much faster because
 * sources that return RDD[Row] need to be converted to RDD[InternalRow], and the
 * conversion is horribly expensive, especially for primitives and strings.
 * The only downside is that the API is not stable.  :/
 */
class SparkRowReader(dataset: Dataset, columnIDs: Seq[Int]) extends BaseGenericInternalRow {
  import Column.ColumnType
  var filoRowReader: RowReader = _

  // determine which columns are Timestamp
  val timestampPositions = columnIDs.zipWithIndex.collect {
    case (colID, pos) if dataset.columnFromID(colID).columnType == ColumnType.TimestampColumn => pos
  }.toSet

  final def notNull(columnNo: Int): Boolean = filoRowReader.notNull(columnNo)

  override final def getBoolean(columnNo: Int): Boolean = filoRowReader.getBoolean(columnNo)
  override final def getInt(columnNo: Int): Int = filoRowReader.getInt(columnNo)
  override final def getLong(columnNo: Int): Long =
    if (timestampPositions.contains(columnNo)) { filoRowReader.getLong(columnNo) * 1000 }
    else                                       { filoRowReader.getLong(columnNo) }
  override final def getDouble(columnNo: Int): Double = filoRowReader.getDouble(columnNo)
  override final def getFloat(columnNo: Int): Float = filoRowReader.getFloat(columnNo)

  override final def getUTF8String(columnNo: Int): UTF8String = {
    val utf8 = filoRowReader.filoUTF8String(columnNo)
    UTF8String.fromAddress(utf8.base, utf8.offset, utf8.numBytes)
  }

  final def getAny(columnNo: Int): Any = genericGet(columnNo)
  final def genericGet(columnNo: Int): Any = filoRowReader.getAny(columnNo) match {
    case z: ZeroCopyUTF8String => UTF8String.fromAddress(z.base, z.offset, z.numBytes)
    case o: Any                => o
    // scalastyle:off
    case null                  => null
    // scalastyle:on
  }

  override final def isNullAt(i: Int): Boolean = !notNull(i)
  def numFields: Int = columnIDs.length

  // Only used for pure select() queries, doesn't need to be fast?
  def copy(): InternalRow =
    new GenericInternalRow((0 until numFields).map(genericGet).toArray)
}
