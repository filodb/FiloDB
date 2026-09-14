package filodb.coordinator.flight

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.util.{DefaultClassResolver, DefaultStreamFactory, ListReferenceResolver}
import io.altoo.akka.serialization.kryo.serializer.scala._
import org.jctools.queues.MpmcArrayQueue
import org.objenesis.strategy.StdInstantiatorStrategy

import filodb.coordinator.client.KryoInit

object FlightKryoSerDeser {

  private val minBufSize = 16000

  case class KryoCtx(kryo: Kryo, out: Output)
  class KryoCtxPool(initCapacity: Int) {
    private val q = new MpmcArrayQueue[KryoCtx](initCapacity)

    def borrow(): KryoCtx = {
      val ctx = q.poll()
      if (ctx != null) ctx
      else {
        val k = new ScalaKryo(new DefaultClassResolver(), new ListReferenceResolver(), new DefaultStreamFactory())
        k.setClassLoader(getClass.getClassLoader)
        k.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy))

        val ki = new KryoInit()
        ki.preInit(k)
        initScalaSerializer(k)
        initFilodbClasses(k)
        otherInit(k)
        ki.postInit(k)
        // register serializers here
        KryoCtx(k, new Output(minBufSize, -1))
      }
    }

    def release(ctx: KryoCtx): Unit = {
      ctx.out.clear()
      ctx.kryo.reset()
      q.offer(ctx)
      // consider dropping the instance and not adding back to pool if buffer is over a capacity limit
    }
  }

  val kryoPool = new KryoCtxPool(Runtime.getRuntime.availableProcessors() * 2)

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

    // Register Guava classes used by Arrow Flight exceptions
    k.register(classOf[com.google.common.collect.LinkedListMultimap[_, _]])
    k.register(classOf[org.apache.arrow.flight.FlightRuntimeException])
    k.register(classOf[org.apache.arrow.flight.CallStatus])
    k.register(classOf[org.apache.arrow.flight.ErrorFlightMetadata])
  }

  def initFilodbClasses(k: Kryo): Unit = {
    // Exec plans
    k.register(classOf[filodb.query.exec.MultiSchemaPartitionsExec])
    k.register(classOf[filodb.query.exec.LocalPartitionReduceAggregateExec])
    k.register(classOf[filodb.query.exec.MultiPartitionReduceAggregateExec])
    k.register(classOf[filodb.query.exec.BinaryJoinExec])
    k.register(classOf[filodb.query.exec.LocalPartitionDistConcatExec])
    k.register(classOf[filodb.query.exec.MultiPartitionDistConcatExec])
    k.register(classOf[filodb.query.exec.PeriodicSamplesMapper])
    k.register(classOf[filodb.query.exec.InstantVectorFunctionMapper])
    k.register(classOf[filodb.query.exec.ScalarOperationMapper])
    k.register(classOf[filodb.query.exec.AggregateMapReduce])
    k.register(classOf[filodb.query.exec.AggregatePresenter])
    // TODO all query plans

    // Flight Objects
  }

  def deserialize(bytes: Array[Byte]): Any = {
    val k = kryoPool.borrow()
    try {
      Using.resource(new Input(bytes)) { input =>
        k.kryo.readClassAndObject(input)
      }
    } finally {
      kryoPool.release(k)
    }
  }

  def serializeToBytes(obj: Any): Array[Byte] = {
    val k = kryoPool.borrow()
    try {
        k.kryo.writeClassAndObject(k.out, obj)
        k.out.toBytes
    } finally {
      kryoPool.release(k)
    }
  }

}
