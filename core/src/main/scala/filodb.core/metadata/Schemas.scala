package filodb.core.metadata

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core.binaryrecord2.{RecordBuilder, RecordComparator, RecordSchema}
import filodb.core.downsample.ChunkDownsampler
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkSetInfo
import filodb.core.Types._
import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format.BinaryVector

/**
 * A DataSchema describes the data columns within a time series - the actual data that would vary from sample to
 * sample and is encoded.  It has a unique hash code for each unique DataSchema.
 * One Dataset in FiloDB can comprise multiple DataSchemas.
 * One DataSchema should be used for each type of metric or data, such as gauge, histogram, etc.
 * The "timestamp" or rowkey is the first column and must be either a LongColumn or TimestampColumn.
 * DataSchemas are intended to be built from config through Schemas.
 */
final case class DataSchema private(name: String,
                                    columns: Seq[Column],
                                    downsamplers: Seq[ChunkDownsampler],
                                    hash: Int,
                                    valueColumn: ColumnId) {
  val timestampColumn  = columns.head
  val timestampColID   = 0

  // Used to create a `VectorDataReader` of correct type for a given data column ID;  type PtrToDataReader
  val readers          = columns.map(col => BinaryVector.defaultPtrToReader(col.columnType.clazz)).toArray

  // The number of bytes of chunkset metadata including vector pointers in memory
  val chunkSetInfoSize = ChunkSetInfo.chunkSetInfoSize(columns.length)
  val blockMetaSize    = chunkSetInfoSize + 4
}

/**
 * A PartitionSchema is the schema describing the unique "key" of each time series, such as labels.
 * The columns inside PartitionSchema are used for distribution and sharding, as well as filtering and searching
 * for time series during querying.
 * There should only be ONE PartitionSchema across the entire Database.
 */
final case class PartitionSchema(columns: Seq[Column],
                                 predefinedKeys: Seq[String],
                                 options: DatasetOptions) {
  val binSchema = new RecordSchema(columns.map(c => ColumnInfo(c.name, c.columnType)), Some(0), predefinedKeys)

  private val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, binSchema, 10240)

  /**
   * Creates a PartitionKey (BinaryRecord v2) from individual parts.  Horribly slow, use for testing only.
   */
  def partKey(parts: Any*): Array[Byte] = {
    val offset = partKeyBuilder.addFromObjects(parts: _*)
    val bytes = binSchema.asByteArray(partKeyBuilder.allContainers.head.base, offset)
    partKeyBuilder.reset()
    bytes
  }
}

object DataSchema {
  import Dataset._

  def validateValueColumn(dataColumns: Seq[Column], valueColName: String): ColumnId Or BadSchema = {
    val index = dataColumns.indexWhere(_.name == valueColName)
    if (index < 0) Bad(BadColumnName(valueColName, s"$valueColName not a valid data column"))
    else           Good(index)
  }

  /**
   * Generates a unique 16-bit hash from the column names and types.  Sensitive to order.
   */
  def genHash(columns: Seq[Column]): Int = {
    var hash = 7
    for { col <- columns } {
      // Use XXHash to get high quality hash for column name.  String.hashCode is _horrible_
      hash = 31 * hash + (BinaryRegion.hash32(col.name.getBytes) * col.columnType.hashCode)
    }
    hash & 0x0ffff
  }

  /**
   * Creates and validates a new DataSchema
   * @param name The name of the schema
   * @param dataColNameTypes list of data columns in name:type[:params] form
   * @return Good(Dataset) or Bad(BadSchema)
   */
  def make(name: String,
           dataColNameTypes: Seq[String],
           downsamplerNames: Seq[String] = Seq.empty,
           valueColumn: String): DataSchema Or BadSchema = {

    for { dataColumns  <- Column.makeColumnsFromNameTypeList(dataColNameTypes)
          downsamplers <- validateDownsamplers(downsamplerNames)
          valueColID   <- validateValueColumn(dataColumns, valueColumn)
          _ <- validateTimeSeries(dataColumns, Seq(0)) }
    yield {
      DataSchema(name, dataColumns, downsamplers, genHash(dataColumns), valueColID)
    }
  }

  /**
   * Parses a DataSchema from config object, like this:
   * {{{
   *   {
   *     prometheus {
   *       columns = ["timestamp:ts", "value:double:detectDrops=true"]
   *       value-column = "value"
   *       downsamplers = []
   *     }
   *   }
   * }}}
   *
   * From the example above, pass in "prometheus" as the schemaName.
   * It is advisable to parse the outer config of all schemas using `.as[Map[String, Config]]`
   */
  def fromConfig(schemaName: String, conf: Config): DataSchema Or BadSchema =
    make(schemaName,
         conf.as[Seq[String]]("columns"),
         conf.as[Seq[String]]("downsamplers"),
         conf.getString("value-column"))
}

object PartitionSchema {
  import Dataset._

  /**
   * Creates and validates a new PartitionSchema
   * @param partColNameTypes list of partition columns in name:type[:params] form
   * @param options
   * @param predefinedKeys
   * @return Good(Dataset) or Bad(BadSchema)
   */
  def make(partColNameTypes: Seq[String],
           options: DatasetOptions,
           predefinedKeys: Seq[String] = Seq.empty): PartitionSchema Or BadSchema = {

    for { partColumns  <- Column.makeColumnsFromNameTypeList(partColNameTypes, PartColStartIndex)
         _             <- validateMapColumn(partColumns, Nil) }
    yield {
      PartitionSchema(partColumns, predefinedKeys, options)
    }
  }

  /**
   * Parses a PartitionSchema from config.  Format:
   * {{{
   *   columns = ["tags:map"]
   *   predefined-keys = ["_ns", "app", "__name__", "instance", "dc"]
   *   options {
   *     ...  # See DatasetOptions parsing format
   *   }
   * }}}
   */
  def fromConfig(partConfig: Config): PartitionSchema Or BadSchema =
    make(partConfig.as[Seq[String]]("columns"),
         DatasetOptions.fromConfig(partConfig.getConfig("options")),
         partConfig.as[Option[Seq[String]]]("predefined-keys").getOrElse(Nil))
}

/**
 * A Schema combines a PartitionSchema with a DataSchema, forming all the columns of a single ingestion record.
 */
final case class Schema(partition: PartitionSchema, data: DataSchema) {
  val allColumns = data.columns ++ partition.columns
  val ingestionSchema = new RecordSchema(allColumns.map(c => ColumnInfo(c.name, c.columnType)),
                                         Some(data.columns.length),
                                         partition.predefinedKeys)

  val comparator      = new RecordComparator(ingestionSchema)
  val partKeySchema   = comparator.partitionKeySchema
}

/**
 * Singleton with code to load all schemas from config, verify no conflicts, and ensure there is only
 * one PartitionSchema.
 */
object Schemas {
  // function to initialize all the data schemas and partition schemas
}