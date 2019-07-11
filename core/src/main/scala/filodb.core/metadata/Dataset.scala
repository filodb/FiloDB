package filodb.core.metadata

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core._
import filodb.core.binaryrecord2._
import filodb.core.downsample.ChunkDownsampler
import filodb.core.query.ColumnInfo
import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format.{RowReader, TypedIterator, ZeroCopyUTF8String => ZCUTF8}

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
 * NOTE: this data structure will be deprecated slowly in favor of PartitionSchema/DataSchema.
 * NOTE2: name is used for ingestion stream name, which is separate from the name of the schema.
 *
 * The Column IDs (used for querying) for data columns are numbered starting with 0, and for partition
 * columns are numbered starting with PartColStartIndex.  This means position is the same or easily derived
 *
 * The rowKeyIDs are the dataColumns IDs/positions for the "row key", typically a timestamp column but
 * something which makes a value unique within a partition and describes a range of data in a chunk.
 */
final case class Dataset(name: String, schema: Schema) {
  def options: DatasetOptions = schema.partition.options
  def dataColumns: Seq[Column] = schema.data.columns
  def partitionColumns: Seq[Column] = schema.partition.columns

  val ref = DatasetRef(name, None)
  val rowKeyColumns   = schema.data.columns take 1

  val ingestionSchema = schema.ingestionSchema
  val comparator      = schema.comparator
  val partKeySchema   = schema.partKeySchema

  // Used to create a `VectorDataReader` of correct type for a given data column ID;  type PtrToDataReader
  val dataReaders     = schema.data.readers
  val numDataColumns  = schema.data.columns.length

  // Used for ChunkSetReader.binarySearchKeyChunks
  val rowKeyOrdering = CompositeReaderOrdering(rowKeyColumns.map(_.columnType.keyType))

  val timestampColumn = rowKeyColumns.head
  val timestampColID  = timestampColumn.id

  // The number of bytes of chunkset metadata including vector pointers in memory
  val chunkSetInfoSize = schema.data.chunkSetInfoSize
  val blockMetaSize    = schema.data.blockMetaSize

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, partKeySchema, 10240)

  /**
   * Creates a PartitionKey (BinaryRecord v2) from individual parts.  Horribly slow, use for testing only.
   */
  def partKey(parts: Any*): Array[Byte] = {
    val offset = partKeyBuilder.addFromObjects(parts: _*)
    val bytes = partKeySchema.asByteArray(partKeyBuilder.allContainers.head.base, offset)
    partKeyBuilder.reset()
    bytes
  }

  import Column.ColumnType._

  /**
   * Creates a TypedIterator for querying a constant partition key column.
   */
  def partColIterator(columnID: Int, base: Any, offset: Long): TypedIterator = {
    val partColPos = columnID - Dataset.PartColStartIndex
    require(Dataset.isPartitionID(columnID) && partColPos < schema.partition.columns.length)
    schema.partition.columns(partColPos).columnType match {
      case StringColumn => new PartKeyUTF8Iterator(partKeySchema, base, offset, partColPos)
      case LongColumn   => new PartKeyLongIterator(partKeySchema, base, offset, partColPos)
      case TimestampColumn => new PartKeyLongIterator(partKeySchema, base, offset, partColPos)
      case other: Column.ColumnType => ???
    }
  }

  /**
   * Extracts a timestamp out of a RowReader, assuming data columns are first (ingestion order)
   */
  final def timestamp(dataRowReader: RowReader): Long = dataRowReader.getLong(0)

  import Accumulation._
  import OptionSugar._
  /**
   * Returns the column IDs for the named columns or the missing column names
   */
  def colIDs(colNames: String*): Seq[Int] Or Seq[String] =
    colNames.map { n => schema.data.columns.find(_.name == n).map(_.id)
                          .orElse { schema.partition.columns.find(_.name == n).map(_.id) }
                          .toOr(One(n)) }
            .combined.badMap(_.toSeq)

  /**
   * Given a list of column names representing say CSV columns, returns a routing from each data column
   * in this dataset to the column number in that input column name list.  To be used for RoutingRowReader
   * over the input RowReader to return data columns corresponding to dataset definition.
   */
  def dataRouting(colNames: Seq[String]): Array[Int] =
    schema.data.columns.map { c => colNames.indexOf(c.name) }.toArray

  /**
   * Returns a routing from data + partition columns (as required for ingestion BinaryRecords) to
   * the input RowReader columns whose names are passed in.
   */
  def ingestRouting(colNames: Seq[String]): Array[Int] =
    dataRouting(colNames) ++ schema.partition.columns.map { c => colNames.indexOf(c.name) }

  /** Returns the Column instance given the ID */
  def columnFromID(columnID: Int): Column =
    if (Dataset.isPartitionID(columnID)) { schema.partition.columns(columnID - Dataset.PartColStartIndex) }
    else                                 { schema.data.columns(columnID) }

  /** Returns ColumnInfos from a set of column IDs.  Throws exception if ID is invalid */
  def infosFromIDs(ids: Seq[Types.ColumnId]): Seq[ColumnInfo] =
    ids.map(columnFromID).map { c => ColumnInfo(c.name, c.columnType) }
}

/**
 * Config options for a table define operational details for the column store and memtable.
 * Every option must have a default!
 */
case class DatasetOptions(shardKeyColumns: Seq[String],
                          metricColumn: String,
                          // TODO: deprecate these options once we move all input to Telegraf/Influx
                          // They are needed only to differentiate raw Prometheus-sourced data
                          ignoreShardKeyColumnSuffixes: Map[String, Seq[String]] = Map.empty,
                          ignoreTagsOnPartitionKeyHash: Seq[String] = Nil,
                          // For each key, copy the tag to the value if the value is absent
                          copyTags: Map[String, String] = Map.empty) {
  override def toString: String = {
    toConfig.root.render(ConfigRenderOptions.concise)
  }

  def toConfig: Config = {
    val map: Map[String, Any] = Map(
      "shardKeyColumns" -> shardKeyColumns.asJava,
      "metricColumn" -> metricColumn,
      "ignoreShardKeyColumnSuffixes" ->
        ignoreShardKeyColumnSuffixes.mapValues(_.asJava).asJava,
      "ignoreTagsOnPartitionKeyHash" -> ignoreTagsOnPartitionKeyHash.asJava,
      "copyTags" -> copyTags.asJava)
    ConfigFactory.parseMap(map.asJava)
  }

  val nonMetricShardColumns = shardKeyColumns.filterNot(_ == metricColumn).sorted
  val nonMetricShardKeyBytes = nonMetricShardColumns.map(_.getBytes).toArray
  val nonMetricShardKeyUTF8 = nonMetricShardColumns.map(ZCUTF8.apply).toArray
  val nonMetricShardKeyHash = nonMetricShardKeyBytes.map(BinaryRegion.hash32)
  val ignorePartKeyHashTags = ignoreTagsOnPartitionKeyHash.toSet

  val metricBytes = metricColumn.getBytes
  val metricHash = BinaryRegion.hash32(metricBytes)
}

object DatasetOptions {
  val DefaultOptions = DatasetOptions(shardKeyColumns = Nil,
                                      metricColumn = "__name__",
                                      // defaults that work well for Prometheus
                                      ignoreShardKeyColumnSuffixes =
                                        Map("__name__" -> Seq("_bucket", "_count", "_sum")),
                                      ignoreTagsOnPartitionKeyHash = Seq("le"))
  val DefaultOptionsConfig = ConfigFactory.parseString(DefaultOptions.toString)

  def fromString(s: String): DatasetOptions =
    fromConfig(ConfigFactory.parseString(s).withFallback(DefaultOptionsConfig))

  def fromConfig(config: Config): DatasetOptions =
    DatasetOptions(shardKeyColumns = config.as[Seq[String]]("shardKeyColumns"),
                   metricColumn = config.getString("metricColumn"),
                   ignoreShardKeyColumnSuffixes =
                     config.as[Map[String, Seq[String]]]("ignoreShardKeyColumnSuffixes"),
                   ignoreTagsOnPartitionKeyHash = config.as[Seq[String]]("ignoreTagsOnPartitionKeyHash"),
                   copyTags = config.as[Map[String, String]]("copyTags"))
}

/**
 * Contains many helper functions especially pertaining to Dataset creation and validation.
 */
object Dataset {
  val rowKeyIDs = Seq(0)    // First or timestamp column is always the row keys

  /**
   * Creates a new Dataset with various options
   *
   * @param name The name of the dataset
   * @param partitionColumns list of partition columns in name:type form
   * @param dataColumns list of data columns in name:type form
   * @return a Dataset, or throws an exception if a dataset cannot be created
   */
  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String],
            keyColumns: Seq[String]): Dataset =
    apply(name, partitionColumns, dataColumns, Nil, DatasetOptions.DefaultOptions)

  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String],
            downsamplers: Seq[String], options : DatasetOptions): Dataset =
    make(name, partitionColumns, dataColumns, downsamplers, options).badMap(BadSchemaError).toTry.get

  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String],
            options: DatasetOptions): Dataset =
    apply(name, partitionColumns, dataColumns, Nil, options)

  def apply(name: String,
            partitionColumns: Seq[String],
            dataColumns: Seq[String]): Dataset =
    apply(name, partitionColumns, dataColumns, DatasetOptions.DefaultOptions)

  sealed trait BadSchema
  case class BadDownsampler(msg: String) extends BadSchema
  case class BadColumnType(colType: String) extends BadSchema
  case class BadColumnName(colName: String, reason: String) extends BadSchema
  case class NotNameColonType(nameTypeString: String) extends BadSchema
  case class BadColumnParams(msg: String) extends BadSchema
  case class ColumnErrors(errs: Seq[BadSchema]) extends BadSchema
  case class UnknownRowKeyColumn(keyColumn: String) extends BadSchema
  case class IllegalMapColumn(reason: String) extends BadSchema
  case class NoTimestampRowKey(colName: String, colType: String) extends BadSchema
  case class HashConflict(detail: String) extends BadSchema

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

  def validateTimeSeries(dataColumns: Seq[Column], rowKeyIDs: Seq[Int]): Unit Or BadSchema =
    dataColumns(rowKeyIDs.head).columnType match {
      case Column.ColumnType.LongColumn      => Good(())
      case Column.ColumnType.TimestampColumn => Good(())
      case other: Column.ColumnType          => Bad(NoTimestampRowKey(dataColumns(rowKeyIDs.head).name, other.toString))
    }

  def validateDownsamplers(downsamplers: Seq[String]): Seq[ChunkDownsampler] Or BadSchema = {
    try {
      Good(ChunkDownsampler.downsamplers(downsamplers))
    } catch {
      case e: IllegalArgumentException => Bad(BadDownsampler(e.getMessage))
    }
  }

  // Partition columns have a column ID starting with this number.  This implies there cannot be
  // any more data columns than this number.
  val PartColStartIndex = 0x010000

  final def isPartitionID(columnID: Int): Boolean = columnID >= PartColStartIndex

  /**
   * Creates and validates a new Dataset
   * @param name The name of the dataset
   * @param partitionColNameTypes list of partition columns in name:type[:params] form
   * @param dataColNameTypes list of data columns in name:type[:params] form
   * @param keyColumnNames   the key column names, no :type
   * @return Good(Dataset) or Bad(BadSchema)
   */
  def make(name: String,
           partitionColNameTypes: Seq[String],
           dataColNameTypes: Seq[String],
           downsamplerNames: Seq[String] = Seq.empty,
           options: DatasetOptions = DatasetOptions.DefaultOptions,
           valueColumn: Option[String] = None): Dataset Or BadSchema = {
    // Default value column is the last data column name
    val valueCol = valueColumn.getOrElse(dataColNameTypes.last.split(":").head)
    for { partSchema <- PartitionSchema.make(partitionColNameTypes, options)
          dataSchema <- DataSchema.make(name, dataColNameTypes, downsamplerNames, valueCol) }
    yield { Dataset(name, Schema(partSchema, dataSchema)) }
  }
}
