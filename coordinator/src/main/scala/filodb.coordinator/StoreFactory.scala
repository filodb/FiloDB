package filodb.coordinator

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import filodb.core.Instance
import filodb.core.memstore.{TimeSeriesMemStore, TimeSeriesStore}
import filodb.core.store._

/** Strategies from configuration or type. */
sealed trait StoreStrategy
object StoreStrategy {
  final case class Configured(fqcn: String) extends StoreStrategy

  val TimeSeriesNullSink = Configured(classOf[TimeSeriesNullStoreFactory].getName)
}

/**
  * A StoreFactory creates instances of MemStore and MetaStore one time.  The memStore and metaStore
  * methods should return that created instance every time, not create a new instance.  The implementation
  * should be a class that is passed two params, the FilodbSettings, and a Scheduler for scheduling I/O tasks
  */
trait StoreFactory {
  def memStore: TimeSeriesStore
  def metaStore: MetaStore
  // TODO: explicitly add a method for a ColumnStore or ChunkSink?
}

object StoreFactory extends Instance with StrictLogging {

  /**
   * Initializes the StoreFactory with configuration and a scheduler
   * @param settings a FilodbSettings
   * @param ioPool a Monix Scheduler for scheduling async I/O operations, probably the default I/O pool
   */
  def apply(settings: FilodbSettings, ioPool: Scheduler): StoreFactory =
    settings.StorageStrategy match {
      case StoreStrategy.Configured(fqcn) =>
        val clazz = createClass(fqcn).get
        val args = Seq(
          (classOf[Config] -> settings.config),
          (classOf[Scheduler] -> ioPool))

        createInstance[StoreFactory](clazz, args).get
    }
}

/**
 * TimeSeriesMemstore with a NullChunkSink (no chunks persisted), and in-memory MetaStore.
 * Not what you want for production, but good for getting started and running tests.
 */
class TimeSeriesNullStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  implicit val ioSched = ioPool
  val metaStore = SingleJvmInMemoryStore.metaStore
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, metaStore)
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import filodb.core.GlobalScheduler._
  lazy val metaStore = new InMemoryMetaStore
}
