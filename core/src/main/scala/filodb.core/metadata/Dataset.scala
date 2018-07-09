package filodb.core.metadata

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core._
import filodb.core.binaryrecord2.{RecordBuilder, RecordComparator, RecordSchema => RecordSchema2}
import filodb.core.query.ColumnInfo
import filodb.memory.format.{FiloVector, RowReader}
import filodb.memory.MemFactory

/**
 * A dataset describes the schema (column name & type) and distribution for a stream/set of data.
 * The schema consists of an ordered list of data and partition columns.
 * To create, use Dataset.apply/make so there is validation.
 *
 * A dataset is partitioned, partition columns controls how data is distributed.
 * For time-series data, the partition columns would describe each entity or metric or host or app instance.
 *
 * A typical schema for time series (such as Prometheus):
 *   partition columns:  metricName:string, tags:map
 *   data columns:       timestamp:long, value:double
 *
 * The Column IDs (used for querying) for data columns are numbered starting with 0, and for partition
 * columns are numbered starting with PartColStartIndex.  This means position is the same or easily derived
 *
 * The rowKeyIDs are the dataColumns IDs/positions for the "row key", typically a timestamp column but
 * something which makes a value unique within a partition and describes a range of data in a chunk.
 */
final case class Dataset(name: String,
                         partitionColumns: Seq[Column],
                         dataColumns: Seq[Column],
                         rowKeyIDs: Seq[Int],
                         database: Option[String] = None,
                         options: DatasetOptions = DatasetOptions.DefaultOptions) {
  require(rowKeyIDs.nonEmpty)
  val ref = DatasetRef(name, database)
  val rowKeyColumns   = rowKeyIDs.map(dataColumns)
  val rowKeyRouting   = rowKeyIDs.toArray

  val ingestionSchema = RecordSchema2.ingestion(this)  // TODO: add predefined keys yo
  val comparator      = new RecordComparator(ingestionSchema)
  val partKeySchema   = comparator.partitionKeySchema

  // Used to create a `FiloVector` of correct type for a given data column ID; (ByteBuffer, Int) => FiloVector[_]
  val vectorMakers    = dataColumns.map(col => FiloVector.defaultVectorMaker(col.columnType.clazz)).toArray

  // Used for ChunkSetReader.binarySearchKeyChunks
  val rowKeyOrdering = CompositeReaderOrdering(rowKeyColumns.map(_.columnType.keyType))

  val timestampColumn = rowKeyColumns.head

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, partKeySchema, 10240)
  /**
   * Creates a PartitionKey (BinaryRecord v2) from individual parts
   */
  def partKey(parts: Any*): Array[Byte] = {
    val offset = partKeyBuilder.addFromObjects(parts: _*)
    val bytes = partKeySchema.asByteArray(partKeyBuilder.allContainers.head.base, offset)
    partKeyBuilder.reset()
    bytes
  }

  /**
   * Extracts a timestamp out of a RowReader, assuming data columns are first
   */
  def timestamp(dataRowReader: RowReader): Long = dataRowReader.getLong(rowKeyIDs.head)

  import Accumulation._
  import OptionSugar._
  /**
   * Returns the column IDs for the named columns or the missing column names
   */
  def colIDs(colNames: String*): Seq[Int] Or Seq[String] =
    colNames.map { n => dataColumns.find(_.name == n).map(_.id)
                          .orElse { partitionColumns.find(_.name == n).map(_.id) }
                          .toOr(One(n)) }
            .combined.badMap(_.toSeq)

  /**
   * Given a list of column names representing say CSV columns, returns a routing from each data column
   * in this dataset to the column number in that input column name list.  To be used for RoutingRowReader
   * over the input RowReader to return data columns corresponding to dataset definition.
   */
  def dataRouting(colNames: Seq[String]): Array[Int] =
    dataColumns.map { c => colNames.indexOf(c.name) }.toArray

  /**
   * Returns a routing from data + partition columns (as required for ingestion BinaryRecords) to
   * the input RowReader columns whose names are passed in.
   */
  def ingestRouting(colNames: Seq[String]): Array[Int] =
    dataRouting(colNames) ++ partitionColumns.map { c => colNames.indexOf(c.name) }

  /** Returns the Column instance given the ID */
  def columnFromID(columnID: Int): Column =
    if (Dataset.isPartitionID(columnID)) { partitionColumns(columnID - Dataset.PartColStartIndex) }
    else                                 { dataColumns(columnID) }

  /** Returns ColumnInfos from a set of column IDs.  Throws exception if ID is invalid */
  def infosFromIDs(ids: Seq[Types.ColumnId]): Seq[ColumnInfo] =
    ids.map(columnFromID).map { c => ColumnInfo(c.name, c.columnType) }

  /** Returns a compact String for easy serialization */
  def asCompactString: String =
      Seq(database.getOrElse(""),
          name,
          partitionColumns.map(_.toString).mkString(":"),
          dataColumns.map(_.toString).mkString(":"),
          rowKeyIDs.mkString(":"),
          options.toString).mkString("\u0001")
}

/**
 * Config options for a table define operational details for the column store and memtable.
 * Every option must have a default!
 */
case class DatasetOptions(shardKeyColumns: Seq[String],
                          metricColumn: String,
                          valueColumn: String) {
  override def toString: String = {
    val map: Map[String, Any] = Map(
                   "shardKeyColumns" -> shardKeyColumns.asJava,
                   "metricColumn" -> metricColumn,
                   "valueColumn" -> valueColumn)
    val config = ConfigFactory.parseMap(map.asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  val DefaultOptions = DatasetOptions(shardKeyColumns = Nil,
                                      metricColumn = "__name__",
                                      valueColumn = "value")
  val DefaultOptionsConfig = ConfigFactory.parseString(DefaultOptions.toString)

  def fromString(s: String): DatasetOptions =
    fromConfig(ConfigFactory.parseString(s).withFallback(DefaultOptionsConfig))

  def fromConfig(config: Config): DatasetOptions =
    DatasetOptions(shardKeyColumns = config.as[Seq[String]]("shardKeyColumns"),
                   metricColumn = config.getString("metricColumn"),
                   valueColumn = config.getString("valueColumn"))
}

/**
 * Contains many helper functions especially pertaining to Dataset creation and validation.
 */
object Dataset {
  /**
   * Re-creates a Dataset from the output of `asCompactString`
   */
  def fromCompactString(compactStr: String): Dataset = {
    val Array(database, name, partColStr, dataColStr, rowKeyIndices, optStr) = compactStr.split('\u0001')
    val partitionColumns = partColStr.split(':').toSeq.map(raw => DataColumn.fromString(raw))
    val dataColumns = dataColStr.split(':').toSeq.map(raw => DataColumn.fromString(raw))
    val rowKeyIDs = rowKeyIndices.split(':').toSeq.map(_.toInt)
    val databaseOption = if (database == "") None else Some(database)
    val options = DatasetOptions.fromString(optStr)
    Dataset(name, partitionColumns, dataColumns, rowKeyIDs, databaseOption, options)
  }

  /**
   * Creates a new Dataset with various options
   *
   * @param name The name of the dataset
   * @param partitionColumns list of partition columns in name:type form
   * @param dataColumns list of data columns in name:type form
   * @param keyColumns  the key column names, no :type
   * @return a Dataset, or throws an exception if a dataset cannot be created
   */
  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String],
            keyColumns: Seq[String]): Dataset =
    make(name, partitionColumns, dataColumns, keyColumns).badMap(BadSchemaError).toTry.get

  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String],
            keyColumn: String): Dataset =
    apply(name, partitionColumns, dataColumns, Seq(keyColumn))

  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String]): Dataset =
    apply(name, partitionColumns, dataColumns, "timestamp")

  sealed trait BadSchema
  case class BadColumnType(colType: String) extends BadSchema
  case class BadColumnName(colName: String, reason: String) extends BadSchema
  case class NotNameColonType(nameTypeString: String) extends BadSchema
  case class ColumnErrors(errs: Seq[BadSchema]) extends BadSchema
  case class UnknownRowKeyColumn(keyColumn: String) extends BadSchema
  case class IllegalMapColumn(reason: String) extends BadSchema
  case class NoTimestampRowKey(colName: String, colType: String) extends BadSchema

  case class BadSchemaError(badSchema: BadSchema) extends Exception(badSchema.toString)

  import OptionSugar._

  import Column.ColumnType._

  def validateMapColumn(partColumns: Seq[Column], dataColumns: Seq[Column]): Unit Or BadSchema = {
    // There cannot be a map column in the data columns
    val dataOr = dataColumns.find(_.columnType == MapColumn)
                            .toOr("no map columns in dataColumns").swap
                            .badMap(x => IllegalMapColumn("Cannot have map column in data columns"))

    // A map column must be in the last position only in the partition columns
    def validatePartMapCol(): Unit Or BadSchema = {
      val mapCols = partColumns.filter(_.columnType == MapColumn)
      if (mapCols.length > 1) {
        Bad(IllegalMapColumn("Cannot have more than 1 map column"))
      } else if (mapCols.length == 0) {
        Good(())
      } else {
        val partIndex = partColumns.indexWhere(_.name == mapCols.head.name)
        if (partIndex < 0) {
          Bad(IllegalMapColumn("Map column not in partition columns"))
        } else if (partIndex != partColumns.length - 1) {
          Bad(IllegalMapColumn(s"Map column found in partition key pos $partIndex, but needs to be last"))
        } else {
          Good(())
        }
      }
    }

    for { nothing1 <- dataOr
          nothing2 <- validatePartMapCol() } yield ()
  }

  def getRowKeyIDs(dataColumns: Seq[Column], rowKeyColNames: Seq[String]): Seq[Int] Or BadSchema = {
    val indices = rowKeyColNames.map { rowKeyCol => dataColumns.indexWhere(_.name == rowKeyCol) }
    indices.zip(rowKeyColNames).find(_._1 < 0) match {
      case Some((_, col)) => Bad(UnknownRowKeyColumn(col))
      case None           => Good(indices)
    }
  }

  def validateTimeSeries(dataColumns: Seq[Column], rowKeyIDs: Seq[Int]): Unit Or BadSchema =
    dataColumns(rowKeyIDs.head).columnType match {
      case Column.ColumnType.LongColumn      => Good(())
      case Column.ColumnType.TimestampColumn => Good(())
      case other: Column.ColumnType          => Bad(NoTimestampRowKey(dataColumns(rowKeyIDs.head).name, other.toString))
    }

  // Partition columns have a column ID starting with this number.  This implies there cannot be
  // any more data columns than this number.
  val PartColStartIndex = 0x010000

  final def isPartitionID(columnID: Int): Boolean = columnID >= PartColStartIndex

  /**
   * Creates and validates a new Dataset
   * @param name The name of the dataset
   * @param partitionColNameTypes list of partition columns in name:type form
   * @param dataColNameTypes list of data columns in name:type form
   * @param keyColumnNames   the key column names, no :type
   * @return Good(Dataset) or Bad(BadSchema)
   */
  def make(name: String,
           partitionColNameTypes: Seq[String],
           dataColNameTypes: Seq[String],
           keyColumnNames: Seq[String],
           options: DatasetOptions = DatasetOptions.DefaultOptions): Dataset Or BadSchema =
    for { partColumns <- Column.makeColumnsFromNameTypeList(partitionColNameTypes, PartColStartIndex)
          dataColumns <- Column.makeColumnsFromNameTypeList(dataColNameTypes)
          _           <- validateMapColumn(partColumns, dataColumns)
          rowKeyIDs   <- getRowKeyIDs(dataColumns, keyColumnNames)
          _           <- validateTimeSeries(dataColumns, rowKeyIDs) }
    yield {
      Dataset(name, partColumns, dataColumns, rowKeyIDs, None, options)
    }
}
