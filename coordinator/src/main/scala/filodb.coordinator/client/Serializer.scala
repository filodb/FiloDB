package filodb.coordinator.client

import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io._
import com.esotericsoftware.minlog.Log
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo

import filodb.coordinator.FilodbSettings
import filodb.core._
import filodb.core.binaryrecord2.{RecordSchema => RecordSchema2}
import filodb.core.metadata.{Column, PartitionSchema, Schema, Schemas}
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
class KryoInit extends DefaultKryoInitializer {
  override def postInit(kryo: ScalaKryo): Unit = {
    kryo.addDefaultSerializer(classOf[Column.ColumnType], classOf[ColumnTypeSerializer])
    val colTypeSer = new ColumnTypeSerializer
    Column.ColumnType.values.zipWithIndex.foreach { case (ct, i) => kryo.register(ct.getClass, colTypeSer, 100 + i) }

    kryo.addDefaultSerializer(classOf[RecordSchema2], classOf[RecordSchema2Serializer])
    kryo.addDefaultSerializer(classOf[ZeroCopyUTF8String], classOf[ZeroCopyUTF8StringSerializer])
    kryo.register(classOf[Schema], new SchemaSerializer)
    kryo.register(classOf[PartitionSchema], new PartSchemaSerializer)

    initOtherFiloClasses(kryo)
    initQueryEngine2Classes(kryo)
    kryo.setReferences(true)   // save space by referring to same objects with ordinals
    Log.info("Finished initializing custom Kryo serializers")

    // Default level used by Kryo is 'trace', which is expensive. It always builds the message,
    // even if it gets filtered out by the logging framework.
    Log.WARN()
  }

  def initQueryEngine2Classes(kryo: Kryo): Unit = {
    kryo.register(classOf[QueryCommands.LogicalPlan2Query])
    kryo.register(classOf[filodb.query.QueryResult])
    kryo.register(classOf[filodb.query.QueryError])
    kryo.register(classOf[filodb.query.exec.SelectRawPartitionsExec])
    kryo.register(classOf[filodb.query.exec.LocalPartitionReduceAggregateExec])
    kryo.register(classOf[filodb.query.exec.MultiPartitionReduceAggregateExec])
    kryo.register(classOf[filodb.query.exec.BinaryJoinExec])
    kryo.register(classOf[filodb.query.exec.LocalPartitionDistConcatExec])
    kryo.register(classOf[filodb.query.exec.MultiPartitionDistConcatExec])
    kryo.register(classOf[filodb.query.exec.PeriodicSamplesMapper])
    kryo.register(classOf[filodb.query.exec.InstantVectorFunctionMapper])
    kryo.register(classOf[filodb.query.exec.ScalarOperationMapper])
    kryo.register(classOf[filodb.query.exec.AggregateMapReduce])
    kryo.register(classOf[filodb.query.exec.AggregatePresenter])
    kryo.register(classOf[filodb.core.query.SerializedRangeVector])
    kryo.register(classOf[filodb.core.query.PartitionRangeVectorKey],
                  new PartitionRangeVectorKeySerializer)
    kryo.register(classOf[filodb.core.query.CustomRangeVectorKey])

    // Needed to serialize/deserialize exceptions multiple times (see unit test for example)
    UnmodifiableCollectionsSerializer.registerSerializers(kryo)
  }

  def initOtherFiloClasses(kryo: Kryo): Unit = {
    // Initialize other commonly used FiloDB classes
    kryo.register(classOf[DatasetRef])
    kryo.register(classOf[RecordSchema2])
    kryo.register(classOf[filodb.coordinator.ShardEvent])
    kryo.register(classOf[filodb.coordinator.CurrentShardSnapshot])
    kryo.register(classOf[filodb.coordinator.StatusActor.EventEnvelope])
    kryo.register(classOf[filodb.coordinator.StatusActor.StatusAck])

    import filodb.core.query._
    kryo.register(classOf[PartitionInfo], new PartitionInfoSerializer)
    kryo.register(classOf[ColumnInfo])
    kryo.register(classOf[ColumnFilter])

    import filodb.core.store._
    kryo.register(classOf[ChunkSetInfo])
    kryo.register(WriteBufferChunkScan.getClass)
    kryo.register(AllChunkScan.getClass)
    kryo.register(classOf[TimeRangeChunkScan])
    kryo.register(classOf[FilteredPartitionScan])
    kryo.register(classOf[ShardSplit])

    kryo.register(classOf[QueryCommands.BadQuery])
    kryo.register(classOf[QueryContext])
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

/**
 * A Schema serializer which cheats by assuming that both source and destination will have the same schemas
 * configuration, so we only send the schemaID.  This saves a huge amount of serialization cost.
 */
class SchemaSerializer extends KryoSerializer[Schema] {
  override def read(kryo: Kryo, input: Input, typ: Class[Schema]): Schema = {
    // We have to dynamically obtain the global schemas as we don't know when they will be initialized
    // but for sure when the serialization needs to happen, Akka is up already
    // val schemas = FilodbSettings.global().get.schemas
    val global = FilodbSettings.global()
    val schemas = FilodbSettings.global().get.schemas

    val schemaID = input.readInt
    val schema = schemas(schemaID)
    if (schema == Schemas.UnknownSchema)
      throw new IllegalArgumentException(s"Unknown schema ID $schemaID.  Schema configuration mismatch.")
    schema
  }

  override def write(kryo: Kryo, output: Output, schema: Schema): Unit = {
    output.writeInt(schema.schemaHash)
  }
}

class PartSchemaSerializer extends KryoSerializer[PartitionSchema] {
  override def read(kryo: Kryo, input: Input, typ: Class[PartitionSchema]): PartitionSchema = {
    // We have to dynamically obtain the global schemas as we don't know when they will be initialized
    // but for sure when the serialization needs to happen, Akka is up already
    val schemas = FilodbSettings.globalOrDefault.schemas

    schemas.part
  }

  override def write(kryo: Kryo, output: Output, schema: PartitionSchema): Unit = {
    val schemas = FilodbSettings.globalOrDefault.schemas
    require(schema == schemas.part)
  }
}
