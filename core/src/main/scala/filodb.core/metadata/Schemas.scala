package filodb.core.metadata

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core.binaryrecord2._
import filodb.core.downsample.ChunkDownsampler
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkSetInfo
import filodb.core.Types._
import filodb.memory.BinaryRegion
import filodb.memory.format.{BinaryVector, RowReader, TypedIterator}

/**
 * A DataSchema describes the data columns within a time series - the actual data that would vary from sample to
 * sample and is encoded.  It has a unique hash code for each unique DataSchema.
 * One Dataset in FiloDB can comprise multiple DataSchemas.
 * One DataSchema should be used for each type of metric or data, such as gauge, histogram, etc.
 * The "timestamp" or rowkey is the first column and must be either a LongColumn or TimestampColumn.
 * DataSchemas are intended to be built from config through Schemas.
 * If downsamplers are defined, then the downsampleSchema must also be defined.
 */
final case class DataSchema private(name: String,
                                    columns: Seq[Column],
                                    downsamplers: Seq[ChunkDownsampler],
                                    hash: Int,
                                    valueColumn: ColumnId,
                                    downsampleSchema: Option[String] = None) {
  val timestampColumn  = columns.head
  val timestampColID   = 0

  // Used to create a `VectorDataReader` of correct type for a given data column ID;  type PtrToDataReader
  val readers          = columns.map(col => BinaryVector.defaultPtrToReader(col.columnType.clazz)).toArray

  // The number of bytes of chunkset metadata including vector pointers in memory
  val chunkSetInfoSize = ChunkSetInfo.chunkSetInfoSize(columns.length)
  val blockMetaSize    = chunkSetInfoSize + 4

  def valueColName: String = columns(valueColumn).name
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
}

object DataSchema {
  import Dataset._
  import java.nio.charset.StandardCharsets.UTF_8

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
      hash = 31 * hash + (BinaryRegion.hash32(col.name.getBytes(UTF_8)) * col.columnType.hashCode)
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
           valueColumn: String,
           downsampleSchema: Option[String] = None): DataSchema Or BadSchema = {

    for { dataColumns  <- Column.makeColumnsFromNameTypeList(dataColNameTypes)
          downsamplers <- validateDownsamplers(downsamplerNames, downsampleSchema)
          valueColID   <- validateValueColumn(dataColumns, valueColumn)
          _            <- validateTimeSeries(dataColumns, Seq(0)) }
    yield {
      DataSchema(name, dataColumns, downsamplers, genHash(dataColumns), valueColID, downsampleSchema)
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
   *       downsample-schema = "prom-ds-gauge"   # only if downsamplers is not empty
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
         conf.getString("value-column"),
         conf.as[Option[String]]("downsample-schema"))
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
final case class Schema(partition: PartitionSchema, data: DataSchema, downsample: Option[Schema]) {
  val allColumns = data.columns ++ partition.columns
  val ingestionSchema = new RecordSchema(allColumns.map(c => ColumnInfo(c.name, c.columnType)),
                                         Some(data.columns.length),
                                         partition.predefinedKeys)

  val comparator      = new RecordComparator(ingestionSchema)
  val partKeySchema   = comparator.partitionKeySchema
  val options         = partition.options

  val dataReaders     = data.readers
  val numDataColumns  = data.columns.length

  def name: String = data.name

  import Column.ColumnType._

  /**
   * Creates a TypedIterator for querying a constant partition key column.
   */
  def partColIterator(columnID: Int, base: Any, offset: Long): TypedIterator = {
    val partColPos = columnID - Dataset.PartColStartIndex
    require(Dataset.isPartitionID(columnID) && partColPos < partition.columns.length)
    partition.columns(partColPos).columnType match {
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
}

final case class Schemas(part: PartitionSchema,
                         schemas: Map[String, Schema])

/**
 * Singleton with code to load all schemas from config, verify no conflicts, and ensure there is only
 * one PartitionSchema.   Config schema:
 * {{{
 *   filodb {
 *     partition-schema {
 *       columns = ["tags:map"]
 *     }
 *     schemas {
 *       prometheus { ... }
 *       # etc
 *     }
 *   }
 * }}}
 */
object Schemas {
  import Dataset._
  import Accumulation._

  // Validates all the data schemas from config, including checking hash conflicts, and returns all errors found
  // and that any downsample-schemas are valid
  def validateDataSchemas(schemas: Map[String, Config]): Seq[DataSchema] Or Seq[(String, BadSchema)] = {
    // get all data schemas parsed, combining errors
    val parsed = schemas.toSeq.map { case (schemaName, schemaConf) =>
                   DataSchema.fromConfig(schemaName, schemaConf)
                             .badMap(err => One((schemaName, err)))
                 }.combined.badMap(_.toSeq)

    // Check for no hash conflicts
    parsed.filter { schemas =>
      val uniqueHashes = schemas.map(_.hash).toSet
      if (uniqueHashes.size == schemas.length) Pass
      else Fail(Seq(("", HashConflict(s"${schemas.length - uniqueHashes.size + 1} schemas have the same hash"))))
    }.filter { schemas =>
      // check that downsample-schemas point to valid schemas (and should not point at itself, no loops!)
      // TODO: also check that the downsample schema matches downsamplers
      val schemaMap = schemas.map { s => s.name -> s }.toMap
      val badDsSchemas = schemas.filterNot { s =>
        s.downsampleSchema.map(ds => ds != s.name && schemaMap.contains(ds)).getOrElse(true)
      }
      if (badDsSchemas.isEmpty) Pass
      else Fail(badDsSchemas.map { s =>
        (s.name, BadDownsampler(s"Schema ${s.name} has invalid downsample-schema ${s.downsampleSchema}"))})
    }
  }

  /**
   * Parse and initialize all the data schemas and single partition schema from config.
   * Verifies that all of the schemas are conflict-free (no conflicting hash) and config parses correctly.
   * @param config a Config object at the filodb config level, ie "partition-schema" is an entry
   */
  def fromConfig(config: Config): Schemas Or Seq[(String, BadSchema)] = {
    val schemas = new collection.mutable.HashMap[String, Schema]

    def addSchema(part: PartitionSchema, schemaName: String, datas: Seq[DataSchema]): Schema = {
      val data = datas.find(_.name == schemaName).get
      schemas.getOrElseUpdate(data.name, {
        Schema(part, data, data.downsampleSchema.map(dsName => addSchema(part, dsName, datas)))
      })
    }

    for {
      partSchema <- PartitionSchema.fromConfig(config.getConfig("partition-schema"))
                                   .badMap(e => Seq(("<partition>", e)))
      dataSchemas <- validateDataSchemas(config.as[Map[String, Config]]("schemas"))
    } yield {
      dataSchemas.foreach { schema => addSchema(partSchema, schema.name, dataSchemas) }
      Schemas(partSchema, schemas.toMap)
    }
  }
}