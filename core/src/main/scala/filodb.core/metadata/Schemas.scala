package filodb.core.metadata

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core.GlobalConfig
import filodb.core.Types._
import filodb.core.binaryrecord2._
import filodb.core.downsample.{ChunkDownsampler, DownsamplePeriodMarker}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkSetInfo
import filodb.memory.BinaryRegion
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr

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
                                    downsampleSchema: Option[String] = None,
                                    downsamplePeriodMarker: DownsamplePeriodMarker) {
  val timestampColumn  = columns.head

  // Used to create a `VectorDataReader` of correct type for a given data column ID
  def reader(colId: Int, acc: MemoryReader, addr: BinaryVectorPtr): VectorDataReader = {
    BinaryVector.reader(columns(colId).columnType.clazz, acc, addr)
  }

  // The number of bytes of chunkset metadata including vector pointers in memory
  val chunkSetInfoSize = ChunkSetInfo.chunkSetInfoSize(columns.length)
  val blockMetaSize    = chunkSetInfoSize + 4

  def valueColName: String = columns(valueColumn).name

  final def timestamp(dataRowReader: RowReader): Long = dataRowReader.getLong(0)
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
  val hash = Schemas.genHash(columns)
}

object DataSchema {
  import Dataset._

  val timestampColID = 0

  def validateValueColumn(dataColumns: Seq[Column], valueColName: String): ColumnId Or BadSchema = {
    val index = dataColumns.indexWhere(_.name == valueColName)
    if (index < 0) Bad(BadColumnName(valueColName, s"$valueColName not a valid data column"))
    else           Good(index)
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
           dowsamplerPeriodMarker: Option[String] = None,
           valueColumn: String,
           downsampleSchema: Option[String] = None): DataSchema Or BadSchema = {

    for { dataColumns  <- Column.makeColumnsFromNameTypeList(dataColNameTypes)
          downsamplers <- validateDownsamplers(downsamplerNames, downsampleSchema)
          periodMarker <- validatedDownsamplerPeriodMarker(dowsamplerPeriodMarker)
          valueColID   <- validateValueColumn(dataColumns, valueColumn)
          _            <- validateTimeSeries(dataColumns, Seq(0)) }
    yield {
      DataSchema(name, dataColumns, downsamplers, Schemas.genHash(dataColumns),
                 valueColID, downsampleSchema, periodMarker)
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
         conf.as[Option[String]]("downsample-period-marker"),
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
 *
 * The downsample member is a var because setting it at construction time can potentially result be impossible
 * when downsample schema is same as raw schema
 *
 * Important Note: Serialization will be tricky since there may be loops in object graph. Avoid if possible.
 */
final case class Schema(partition: PartitionSchema, data: DataSchema, var downsample: Option[Schema] = None) {
  val allColumns = data.columns ++ partition.columns
  val ingestionSchema = new RecordSchema(allColumns.map(c => ColumnInfo(c.name, c.columnType)),
                                         Some(data.columns.length),
                                         partition.predefinedKeys)

  val comparator      = new RecordComparator(ingestionSchema)
  val partKeySchema   = comparator.partitionKeySchema
  val options         = partition.options

  val numDataColumns  = data.columns.length
  val partitionInfos  = partition.columns.map(ColumnInfo.apply)
  val dataInfos       = data.columns.map(ColumnInfo.apply)

  // A unique hash of the partition and data schemas together. Use for BinaryRecords etc.
  val schemaHash      = (partition.hash + 31 * data.hash) & 0x0ffff

  def name: String = data.name

  /**
    * Fetches reader for a binary vector
    */
  def dataReader(colId: Int, acc: MemoryReader, addr: BinaryVectorPtr): VectorDataReader =
    data.reader(colId, acc, addr)

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
   * Given a list of column names representing say CSV columns, returns a routing from each data column
   * in this dataset to the column number in that input column name list.  To be used for RoutingRowReader
   * over the input RowReader to return data columns corresponding to dataset definition.
   */
  def dataRouting(colNames: Seq[String]): Array[Int] =
    data.columns.map { c => colNames.indexOf(c.name) }.toArray

  /**
   * Returns a routing from data + partition columns (as required for ingestion BinaryRecords) to
   * the input RowReader columns whose names are passed in.
   */
  def ingestRouting(colNames: Seq[String]): Array[Int] =
    dataRouting(colNames) ++ partition.columns.map { c => colNames.indexOf(c.name) }

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
    colNames.map { n => data.columns.find(_.name == n).map(_.id)
                          .orElse { partition.columns.find(_.name == n).map(_.id) }
                          .toOr(One(n)) }
            .combined.badMap(_.toSeq)

  /** Returns the Column instance given the ID */
  def columnFromID(columnID: Int): Column =
    if (Dataset.isPartitionID(columnID)) { partition.columns(columnID - Dataset.PartColStartIndex) }
    else                                 { data.columns(columnID) }

  /** Returns ColumnInfos from a set of column IDs.  Throws exception if ID is invalid */
  def infosFromIDs(ids: Seq[ColumnId]): Seq[ColumnInfo] =
    ids.map(columnFromID).map { c => ColumnInfo(c.name, c.columnType) }

  override final def toString: String = {
    s"Schema(partition=$partition, data=$data, downsample=${downsample.map(_.name)})"
    // overriden since serializing downsample schema may result in infinite loop
  }

}

final case class Schemas(part: PartitionSchema,
                         schemas: Map[String, Schema]) {
  // A very fast array of the schemas by schemaID.  Since schemaID=16 bits, we just use a single 64K element array
  // for super fast lookup.  Schemas object should really be a singleton anyways.
  private val _schemas = Array.fill(64*1024)(Schemas.UnknownSchema)

  schemas.values.foreach { s => _schemas(s.schemaHash) = s }

  /**
    * This is purely a SWAG to be used for query size estimation. Do not rely for other use cases.
    */
  private val bytesPerSampleSwag: Map[Int, Double] = {

    val allSchemas = schemas.values ++ schemas.values.flatMap(_.downsample)
    allSchemas.map { s  =>
      val est = s.data.columns.map(_.columnType).map {
        case ColumnType.LongColumn => 1
        case ColumnType.IntColumn => 1
        case ColumnType.TimestampColumn => 0.5
        case ColumnType.HistogramColumn => 20
        case ColumnType.DoubleColumn => 2
        case _ => 0 // TODO allow without sizing for now
      }.sum
      s.schemaHash -> est
    }.toMap
  }

  private def bytesPerSampleSwagString = bytesPerSampleSwag.map { case e =>
    schemaName(e._1) + ": " + e._2
  }

  Schemas._log.info(s"bytesPerSampleSwag: $bytesPerSampleSwagString")

  /**
    * Note this approach below assumes the following for quick size estimation. The sizing is more
    * a swag than reality:
    * (a) every matched time series ingests at all query times. Looking up start/end times and more
    *     precise size estimation is costly
    * (b) it also assigns bytes per sample based on schema which is much of a swag. In reality, it would depend on
    *     number of histogram buckets, samples per chunk etc.
    */
  def ensureQueriedDataSizeWithinLimit(schemaId: Int,
                                       numTsPartitions: Int,
                                       chunkDurationMillis: Long,
                                       resolutionMs: Long,
                                       queryDurationMs: Long,
                                       dataSizeLimit: Long): Unit = {
    val numSamplesPerChunk = chunkDurationMillis / resolutionMs
    // find number of chunks to be scanned. Ceil division needed here
    val numChunksPerTs = (queryDurationMs + chunkDurationMillis - 1) / chunkDurationMillis
    val bytesPerSample = bytesPerSampleSwag(schemaId)
    val estDataSize = bytesPerSample * numTsPartitions * numSamplesPerChunk * numChunksPerTs
    require(estDataSize < dataSizeLimit,
      s"With match of $numTsPartitions time series, estimate of $estDataSize bytes exceeds limit of " +
        s"$dataSizeLimit bytes queried per shard for ${_schemas(schemaId).name} schema. Try one or more of these: " +
        s"(a) narrow your query filters to reduce to fewer than the current $numTsPartitions matches " +
        s"(b) reduce query time range, currently at ${queryDurationMs / 1000 / 60 } minutes")
  }

  /**
   * Returns the Schema for a given schemaID, or UnknownSchema if not found
   */
  final def apply(id: Int): Schema = _schemas(id)

  /**
   * Returns the schema name for given schemaID, or "<unknown>"
   */
  final def schemaName(id: Int): String = {
    val sch = apply(id)
    if (sch == Schemas.UnknownSchema) s"schemaID:$id" else sch.name
  }
}

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
object Schemas extends StrictLogging {
  import java.nio.charset.StandardCharsets.UTF_8
  import Accumulation._
  import Dataset._

  val _log = logger

  val rowKeyIDs = Seq(0)    // First or timestamp column is always the row keys

  val UnknownSchema = UnsafeUtils.ZeroPointer.asInstanceOf[Schema]

  // Easy way to create Schemas from a single Schema, mostly useful for testing
  def apply(sch: Schema): Schemas = Schemas(sch.partition, Map(sch.data.name -> sch))

  /**
   * Generates a unique 16-bit hash from the column names, types, params.  Sensitive to order.
   */
  def genHash(columns: Seq[Column]): Int = {
    var hash = 7
    for { col <- columns } {
      // Use XXHash to get high quality hash for column name.  String.hashCode is _horrible_
      hash = 31 * hash + (BinaryRegion.hash32(col.name.getBytes(UTF_8)) * col.columnType.hashCode + col.params.hashCode)
    }
    hash & 0x0ffff
  }

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
      // check that downsample-schemas point to valid schemas
      // TODO: also check that the downsample schema matches downsamplers
      val schemaMap = schemas.map { s => s.name -> s }.toMap
      val badDsSchemas = schemas.filterNot { s =>
        s.downsampleSchema.forall(ds => schemaMap.contains(ds))
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
        Schema(part, data, None)
      })
    }

    for {
      partSchema <- PartitionSchema.fromConfig(config.getConfig("partition-schema"))
                                   .badMap(e => Seq(("<partition>", e)))
      dataSchemas <- validateDataSchemas(config.as[Map[String, Config]]("schemas"))
    } yield {
      dataSchemas.foreach { schema => addSchema(partSchema, schema.name, dataSchemas) }
      schemas.values.foreach { s =>
        s.downsample = s.data.downsampleSchema.map(dsName => schemas(dsName))
      }
      Schemas(partSchema, schemas.toMap)
    }
  }

  /**
   * Global/universal schemas used for supporting the basic Prometheus / metric types.
   * They are put here so they can be used in Query Engine, testing, etc.
   */
  val global = fromConfig(GlobalConfig.defaultFiloConfig).get
  val gauge = global.schemas("gauge")
  val promCounter = global.schemas("prom-counter")
  val untyped = global.schemas("untyped")
  val promHistogram = global.schemas("prom-histogram")
  val dsGauge = global.schemas("ds-gauge")
}