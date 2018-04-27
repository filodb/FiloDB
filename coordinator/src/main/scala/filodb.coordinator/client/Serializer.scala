package filodb.coordinator.client

import com.esotericsoftware.kryo.{Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io.{BinaryRecordSerializer, BinaryVectorSerializer, Input, Output}
import com.esotericsoftware.kryo.Kryo
import com.typesafe.scalalogging.StrictLogging
import org.boon.primitive.{ByteBuf, InputByteArray}

import filodb.core._
import filodb.core.binaryrecord.{ArrayBinaryRecord, BinaryRecord, RecordSchema}
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.Column
import filodb.memory.format.{BinaryVector, ZeroCopyUTF8String}
import filodb.memory.format.{vectors => BV}

/**
 * Utilities for special serializers for row data (for efficient record routing).
 *
 * We also maintain some global state for mapping a schema hashcode to the actual schema object.  This must
 * be updated externally.  The reason this is decided is because IngestRows is sent between a RowSource
 * client and node ingestor actors, both of whom have exchanged and know the schema already, thus there is
 * no need to pay the price of transmitting the schema itself with every IngestRows object.
 */
object Serializer extends StrictLogging {
  import IngestionCommands.IngestRows

  implicit class RichIngestRows(data: IngestRows) {
    /**
     * Converts a IngestRows into bytes, assuming all RowReaders are in fact BinaryRecords
     */
    def toBytes(): Array[Byte] =
      serializeIngestRows(data)
  }

  def serializeIngestRows(data: IngestRows): Array[Byte] = {
    val buf = ByteBuf.create(1000)
    val (partSchemaHash, dataSchemaHash) = data.rows.headOption match {
      case Some(IngestRecord(p: BinaryRecord, d: BinaryRecord, _)) =>
        (p.schema.hashCode, d.schema.hashCode)
      case other: Any            => (-1, -1)
    }
    buf.writeInt(partSchemaHash)
    buf.writeInt(dataSchemaHash)
    buf.writeMediumString(data.dataset.dataset)
    buf.writeMediumString(data.dataset.database.getOrElse(""))
    buf.writeInt(data.shard)
    buf.writeInt(data.rows.length)
    for { record <- data.rows } {
      record match {
        case IngestRecord(p: BinaryRecord, d: BinaryRecord, offset) =>
          buf.writeMediumByteArray(p.bytes)
          buf.writeMediumByteArray(d.bytes)
          buf.writeLong(offset)
      }
    }
    buf.toBytes
  }

  def fromBinaryIngestRows(bytes: Array[Byte]): IngestRows = {
    val scanner = new InputByteArray(bytes)
    val partSchemaHash = scanner.readInt
    val dataSchemaHash = scanner.readInt
    val dataset = scanner.readMediumString
    val db = Option(scanner.readMediumString).filter(_.length > 0)
    val shard = scanner.readInt
    val numRows = scanner.readInt
    val rows = new collection.mutable.ArrayBuffer[IngestRecord]()
    if (numRows > 0) {
      val partSchema = partSchemaMap.get(partSchemaHash)
      val dataSchema = dataSchemaMap.get(dataSchemaHash)
      if (Option(partSchema).isEmpty) { logger.error(s"Schema with hash $partSchemaHash not found!") }
      for { i <- 0 until numRows } {
        rows += IngestRecord(BinaryRecord(partSchema, scanner.readMediumByteArray),
                             BinaryRecord(dataSchema, scanner.readMediumByteArray),
                             scanner.readLong)
      }
    }
    IngestionCommands.IngestRows(DatasetRef(dataset, db), shard, rows)
  }

  private val partSchemaMap = new java.util.concurrent.ConcurrentHashMap[Int, RecordSchema]
  private val dataSchemaMap = new java.util.concurrent.ConcurrentHashMap[Int, RecordSchema]

  def putPartitionSchema(schema: RecordSchema): Unit = {
    logger.debug(s"Saving partition schema with fields ${schema.fields.toList} and hash ${schema.hashCode}...")
    partSchemaMap.put(schema.hashCode, schema)
  }

  def putDataSchema(schema: RecordSchema): Unit = {
    logger.debug(s"Saving data schema with fields ${schema.fields.toList} and hash ${schema.hashCode}...")
    dataSchemaMap.put(schema.hashCode, schema)
  }
}

/**
 * A special serializer to use the fast binary IngestRows format instead of much slower Java serialization
 * To solve the problem of the serializer not knowing the schema beforehand, it is the responsibility of
 * the DatasetCoordinatorActor to update the schema when it changes.  The hash of the RecordSchema is
 * sent and received.
 */
class IngestRowsSerializer extends akka.serialization.Serializer {
  import Serializer._
  import IngestionCommands.IngestRows

  // This is whether "fromBinary" requires a "clazz" or not
  def includeManifest: Boolean = false

  def identifier: Int = 1001

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case i: IngestRows => i.toBytes()
  }

  def fromBinary(bytes: Array[Byte],
                 clazz: Option[Class[_]]): AnyRef = {
    fromBinaryIngestRows(bytes)
  }
}

/**
 * Register commonly used classes for efficient Kryo serialization.  If this is not done then Kryo might have to
 * send over the FQCN, which wastes tons of space like Java serialization
 * NOTE: top-level classes still need to be configured in Typesafe config in akka.actor.serialization-bindings
 * These are just for the enclosing classes
 *
 * NOTE: for now, we need to explicitly register every BinaryVector class.  This is tedious  :(
 * but the problem is that due to type erasure Kryo cannot tell the difference between a BinaryVector[Int] or [Long]
 * etc.  If a class is not registered then it might get the wrong BinaryVectorSerializer.
 *
 * Registering is better anyhow due to not needing to serialize the entire FQCN.
 *
 * In the future, possible solutions would include:
 * - Embedding an inner type code into the 4-byte FiloVector header
 * - Recreating original class and injecting base, offset, any at runtime is a possibility, but we don't want
 *   to reinstantiate things like GrowableVector, wrappers, and appendable types
 */
class KryoInit {
  def customize(kryo: Kryo): Unit = {
    kryo.addDefaultSerializer(classOf[Column.ColumnType], classOf[ColumnTypeSerializer])
    val colTypeSer = new ColumnTypeSerializer
    Column.ColumnType.values.zipWithIndex.foreach { case (ct, i) => kryo.register(ct.getClass, colTypeSer, 100 + i) }

    val intBinVectSer = new BinaryVectorSerializer[Int]
    kryo.addDefaultSerializer(classOf[BinaryVector[Int]], intBinVectSer)
    kryo.register(classOf[BV.IntBinaryVector], intBinVectSer)
    kryo.register(classOf[BV.MaskedIntBinaryVector], intBinVectSer)
    kryo.register(classOf[BV.MaskedIntAppendingVector], intBinVectSer)
    kryo.register(classOf[BV.IntAppendingVector], intBinVectSer)
    kryo.register(classOf[BV.IntConstVector], intBinVectSer)
    kryo.register(classOf[filodb.memory.format.GrowableVector$mcI$sp], intBinVectSer)

    val longBinVectSer = new BinaryVectorSerializer[Long]
    kryo.register(classOf[BV.DeltaDeltaVector], longBinVectSer)
    kryo.register(classOf[BV.DeltaDeltaConstVector], longBinVectSer)
    kryo.register(classOf[BV.LongBinaryVector], longBinVectSer)
    kryo.register(classOf[BV.MaskedLongBinaryVector], longBinVectSer)
    kryo.register(classOf[BV.MaskedLongAppendingVector], longBinVectSer)
    kryo.register(classOf[BV.LongAppendingVector], longBinVectSer)
    kryo.register(classOf[BV.LongIntWrapper], longBinVectSer)
    kryo.register(classOf[filodb.memory.format.GrowableVector$mcJ$sp], longBinVectSer)

    val doubleBinVectSer = new BinaryVectorSerializer[Double]
    kryo.register(classOf[BV.DoubleBinaryVector], doubleBinVectSer)
    kryo.register(classOf[BV.MaskedDoubleBinaryVector], doubleBinVectSer)
    kryo.register(classOf[BV.MaskedDoubleAppendingVector], doubleBinVectSer)
    kryo.register(classOf[BV.DoubleAppendingVector], doubleBinVectSer)
    kryo.register(classOf[BV.DoubleLongWrapper], doubleBinVectSer)
    kryo.register(classOf[BV.DoubleConstVector], doubleBinVectSer)
    kryo.register(classOf[filodb.memory.format.GrowableVector$mcD$sp], doubleBinVectSer)

    val utf8BinVectSer = new BinaryVectorSerializer[ZeroCopyUTF8String]
    kryo.register(classOf[BV.DictUTF8Vector], utf8BinVectSer)
    kryo.register(classOf[BV.FixedMaxUTF8Vector], utf8BinVectSer)
    kryo.register(classOf[BV.UTF8ConstVector], utf8BinVectSer)

    kryo.addDefaultSerializer(classOf[RecordSchema], classOf[RecordSchemaSerializer])
    kryo.addDefaultSerializer(classOf[BinaryRecord], classOf[BinaryRecordSerializer])

    initOtherFiloClasses(kryo)
    kryo.setReferences(true)   // save space by referring to same objects with ordinals
  }

  def initOtherFiloClasses(kryo: Kryo): Unit = {
    // Initialize other commonly used FiloDB classes
    kryo.register(classOf[DatasetRef])
    kryo.register(classOf[BinaryRecord])
    kryo.register(classOf[ArrayBinaryRecord])
    kryo.register(classOf[RecordSchema])
    kryo.register(classOf[filodb.coordinator.ShardEvent])
    kryo.register(classOf[filodb.coordinator.CurrentShardSnapshot])
    kryo.register(classOf[filodb.coordinator.StatusActor.EventEnvelope])
    kryo.register(classOf[filodb.coordinator.StatusActor.StatusAck])

    import filodb.core.query._
    kryo.register(classOf[PartitionInfo])
    kryo.register(classOf[PartitionVector])
    kryo.register(classOf[Tuple])
    kryo.register(classOf[ColumnInfo])
    kryo.register(classOf[TupleResult])
    kryo.register(classOf[VectorResult])
    kryo.register(classOf[TupleListResult])
    kryo.register(classOf[VectorListResult])
    kryo.register(classOf[ColumnFilter])

    kryo.register(classOf[filodb.query.QueryResult])
    kryo.register(classOf[filodb.query.QueryError])
    kryo.register(classOf[filodb.query.exec.SelectRawPartitionsExec])
    kryo.register(classOf[filodb.query.exec.ReduceAggregateExec])
    kryo.register(classOf[filodb.query.exec.BinaryJoinExec])
    kryo.register(classOf[filodb.query.exec.DistConcatExec])
    kryo.register(classOf[filodb.query.exec.PeriodicSamplesMapper])
    kryo.register(classOf[filodb.query.exec.InstantVectorFunctionMapper])
    kryo.register(classOf[filodb.query.exec.ScalarOperationMapper])
    kryo.register(classOf[filodb.query.exec.AggregateCombiner])
    kryo.register(classOf[filodb.query.exec.AverageMapper])

    import filodb.core.store._
    kryo.register(classOf[ChunkSetInfo])
    kryo.register(LastSampleChunkScan.getClass)
    kryo.register(AllChunkScan.getClass)
    kryo.register(classOf[RowKeyChunkScan])
    kryo.register(classOf[FilteredPartitionScan])
    kryo.register(classOf[ShardSplit])

    kryo.register(classOf[QueryCommands.LogicalPlanQuery])
    kryo.register(classOf[QueryCommands.ExecPlanQuery])
    kryo.register(classOf[QueryCommands.QueryResult])
    kryo.register(classOf[QueryCommands.BadQuery])
    kryo.register(classOf[QueryCommands.QueryOptions])
    kryo.register(classOf[QueryCommands.FilteredPartitionQuery])
  }
}

// All the ColumnTypes are Objects - singletons.  Thus the class info is enough to find the right one.
// No need to actually write anything.  :D :D :D
class ColumnTypeSerializer extends KryoSerializer[Column.ColumnType] {
  override def read(kryo: Kryo, input: Input, typ: Class[Column.ColumnType]): Column.ColumnType =
    Column.clazzToColType(typ)

  override def write(kryo: Kryo, output: Output, colType: Column.ColumnType): Unit = {}
}

class RecordSchemaSerializer extends KryoSerializer[RecordSchema] {
  override def read(kryo: Kryo, input: Input, typ: Class[RecordSchema]): RecordSchema = {
    val colTypesObj = kryo.readClassAndObject(input)
    new RecordSchema(colTypesObj.asInstanceOf[Seq[Column.ColumnType]])
  }

  override def write(kryo: Kryo, output: Output, schema: RecordSchema): Unit = {
    kryo.writeClassAndObject(output, schema.columnTypes)
  }
}
