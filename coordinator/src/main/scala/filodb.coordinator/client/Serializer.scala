package filodb.coordinator.client

import com.esotericsoftware.kryo.{Serializer => KryoSerializer}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io._
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer

import filodb.core._
import filodb.core.binaryrecord.{ArrayBinaryRecord, BinaryRecord, RecordSchema}
import filodb.core.binaryrecord2.{RecordSchema => RecordSchema2}
import filodb.core.metadata.Column
import filodb.core.query.ColumnInfo
import filodb.memory.format.ZeroCopyUTF8String

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

    kryo.addDefaultSerializer(classOf[RecordSchema], classOf[RecordSchemaSerializer])
    kryo.addDefaultSerializer(classOf[RecordSchema2], classOf[RecordSchema2Serializer])
    kryo.addDefaultSerializer(classOf[BinaryRecord], classOf[BinaryRecordSerializer])

    kryo.addDefaultSerializer(classOf[ZeroCopyUTF8String], classOf[ZeroCopyUTF8StringSerializer])

    initOtherFiloClasses(kryo)
    initQueryEngine2Classes(kryo)
    kryo.setReferences(true)   // save space by referring to same objects with ordinals
  }

  def initQueryEngine2Classes(kryo: Kryo): Unit = {
    kryo.register(classOf[QueryCommands.LogicalPlan2Query])
    kryo.register(classOf[filodb.query.QueryResult])
    kryo.register(classOf[filodb.query.QueryError])
    kryo.register(classOf[filodb.query.exec.SelectRawPartitionsExec])
    kryo.register(classOf[filodb.query.exec.ReduceAggregateExec])
    kryo.register(classOf[filodb.query.exec.BinaryJoinExec])
    kryo.register(classOf[filodb.query.exec.DistConcatExec])
    kryo.register(classOf[filodb.query.exec.PeriodicSamplesMapper])
    kryo.register(classOf[filodb.query.exec.InstantVectorFunctionMapper])
    kryo.register(classOf[filodb.query.exec.ScalarOperationMapper])
    kryo.register(classOf[filodb.query.exec.AggregateMapReduce])
    kryo.register(classOf[filodb.query.exec.AggregatePresenter])
    kryo.register(classOf[filodb.core.query.SerializableRangeVector])
    kryo.register(classOf[filodb.core.query.PartitionRangeVectorKey],
                  new PartitionRangeVectorKeySerializer)
    kryo.register(classOf[filodb.core.query.CustomRangeVectorKey])

    // Needed to serialize/deserialize exceptions multiple times (see unit test for example)
    UnmodifiableCollectionsSerializer.registerSerializers(kryo)
  }

  def initOtherFiloClasses(kryo: Kryo): Unit = {
    // Initialize other commonly used FiloDB classes
    kryo.register(classOf[DatasetRef])
    kryo.register(classOf[BinaryRecord])
    kryo.register(classOf[ArrayBinaryRecord])
    kryo.register(classOf[RecordSchema])
    kryo.register(classOf[RecordSchema2])
    kryo.register(classOf[filodb.coordinator.ShardEvent])
    kryo.register(classOf[filodb.coordinator.CurrentShardSnapshot])
    kryo.register(classOf[filodb.coordinator.StatusActor.EventEnvelope])
    kryo.register(classOf[filodb.coordinator.StatusActor.StatusAck])

    import filodb.core.query._
    kryo.register(classOf[PartitionInfo], new PartitionInfoSerializer)
    kryo.register(classOf[Tuple])
    kryo.register(classOf[ColumnInfo])
    kryo.register(classOf[TupleResult])
    kryo.register(classOf[TupleListResult])
    kryo.register(classOf[ColumnFilter])

    import filodb.core.store._
    kryo.register(classOf[ChunkSetInfo])
    kryo.register(WriteBufferChunkScan.getClass)
    kryo.register(AllChunkScan.getClass)
    kryo.register(classOf[RowKeyChunkScan])
    kryo.register(classOf[FilteredPartitionScan])
    kryo.register(classOf[ShardSplit])

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

class RecordSchema2Serializer extends KryoSerializer[RecordSchema2] {
  override def read(kryo: Kryo, input: Input, typ: Class[RecordSchema2]): RecordSchema2 = {
    val tuple = kryo.readClassAndObject(input)
    RecordSchema2.fromSerializableTuple(tuple.asInstanceOf[(Seq[ColumnInfo], Option[Int],
                                                            Seq[String], Map[Int, RecordSchema2])])
  }

  override def write(kryo: Kryo, output: Output, schema: RecordSchema2): Unit = {
    kryo.writeClassAndObject(output, schema.toSerializableTuple)
  }
}
