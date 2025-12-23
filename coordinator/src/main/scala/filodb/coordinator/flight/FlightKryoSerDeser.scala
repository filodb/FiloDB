package filodb.coordinator.flight

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, Input, Output}
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.util.{DefaultClassResolver, DefaultStreamFactory, ListReferenceResolver}
import io.altoo.akka.serialization.kryo.serializer.scala._
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.objenesis.strategy.StdInstantiatorStrategy

import filodb.coordinator.client.KryoInit
import filodb.core.query.{QueryStats, RangeVectorKey, ResultSchema, RvRange}

case class RespHeader(resultSchema: ResultSchema)
case class RvMetadata(rvk: RangeVectorKey, rvRange: Option[RvRange])
case class RespFooter(queryStats: QueryStats, throwable: Option[Throwable])

object FlightKryoSerDeser {

  private val minBufSize = 16000

  private val kryo = new ThreadLocal[Kryo]() {
    override def initialValue(): Kryo = {
      val k = new ScalaKryo(new DefaultClassResolver(), new ListReferenceResolver(), new DefaultStreamFactory())
      k.setClassLoader(getClass.getClassLoader)
      k.register(classOf[RespHeader])
      k.register(classOf[RespFooter])
      k.register(classOf[RvMetadata])
      k.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy))

      val ki = new KryoInit()
      ki.preInit(k)
      initScalaSerializer(k)
      otherInit(k)
      ki.postInit(k)
      k
    }
  }

  def initScalaSerializer(kryo: ScalaKryo): Unit = {
    // Support serialization of some standard or often used Scala classes
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
//    system.dynamicAccess.getClassFor[AnyRef]("scala.Enumeration$Val") match {
//      case Success(clazz) => kryo.register(clazz)
//      case Failure(e) => throw e
//    }
    kryo.register(classOf[scala.Enumeration#Value])

    // identity preserving serializers for Unit and BoxedUnit
    kryo.addDefaultSerializer(classOf[scala.runtime.BoxedUnit], classOf[ScalaUnitSerializer])

    // mutable maps
    kryo.addDefaultSerializer(classOf[scala.collection.mutable.Map[_, _]], classOf[ScalaMutableMapSerializer])

    // immutable maps - specialized by mutable, immutable and sortable
    kryo.addDefaultSerializer(classOf[scala.collection.immutable.SortedMap[_, _]], classOf[ScalaSortedMapSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.immutable.Map[_, _]], classOf[ScalaImmutableMapSerializer])

    // Sets - specialized by mutability and sortability
    kryo.addDefaultSerializer(classOf[scala.collection.immutable.BitSet],
      classOf[FieldSerializer[scala.collection.immutable.BitSet]])
    kryo.addDefaultSerializer(classOf[scala.collection.immutable.SortedSet[_]],
      classOf[ScalaImmutableSortedSetSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.immutable.Set[_]],
      classOf[ScalaImmutableSetSerializer])

    kryo.addDefaultSerializer(classOf[scala.collection.mutable.BitSet],
      classOf[FieldSerializer[scala.collection.mutable.BitSet]])
    kryo.addDefaultSerializer(classOf[scala.collection.mutable.SortedSet[_]], classOf[ScalaMutableSortedSetSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.mutable.Set[_]], classOf[ScalaMutableSetSerializer])

    // Map/Set Factories
//    ScalaVersionSerializers.mapAndSet(kryo)
//    ScalaVersionSerializers.iterable(kryo)
  }


  private def otherInit(k: Kryo): Unit = {
    k.register(Some.getClass, 64)
    k.register(Tuple2.getClass, 65)
    k.register(None.getClass, 66)
    k.register(Nil.getClass, 67)
    k.register(::.getClass, 68)
    k.register(ArrayBuffer.getClass, 69)
    k.register(Vector.getClass, 70)
  }

  def deserialize(bytes: Array[Byte]): Any = {
    val k = kryo.get()
    val input = new Input(bytes)
    try {
      k.readClassAndObject(input)
    } finally {
      input.close()
    }
  }

  def deserialize(buf: ArrowBuf): Any = {
    val k = kryo.get()
    val input = new ByteBufferInput(buf.nioBuffer())
    try {
      k.readClassAndObject(input)
    } finally {
      input.close()
    }
  }

  def serializeToArrowBuf(obj: Any, allocator: BufferAllocator): ArrowBuf = {
    val bytes = serializeToBytes(obj)
    val buf = allocator.buffer(bytes.length)
    try {
      buf.writeBytes(bytes)
      buf.writerIndex(bytes.length).readerIndex(0)
      buf
    } catch {
      case e: Throwable =>
        buf.close()
        throw e
    }
  }

  def serializeToBytes(obj: Any): Array[Byte] = {
    val k = kryo.get()
    Using.resource(new Output(minBufSize, -1)) { output =>
      k.writeClassAndObject(output, obj)
      output.toBytes
    }
  }

}
