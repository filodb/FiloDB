package filodb.coordinator

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import filodb.core.memstore.{MemStore, TimeSeriesMemStore}
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
  def memStore: MemStore
  def metaStore: MetaStore
  // TODO: explicitly add a method for a ColumnStore or ChunkSink?
}

object StoreFactory extends Instance with StrictLogging {

  /**
   * Initializes the StoreFactory with configuration and a scheduler
   * @param settings a FilodbSettings
   * @param sched a Monix Scheduler for scheduling async I/O operations, probably the default I/O pool
   */
  def apply(settings: FilodbSettings, sched: Scheduler): StoreFactory =
    settings.StorageStrategy match {
      case StoreStrategy.Configured(fqcn) =>
        val clazz = createClass(fqcn).get
        val args = Seq(
          (classOf[Config] -> settings.config),
          (classOf[Scheduler] -> sched))

        createInstance[StoreFactory](clazz, args).get
    }
}

/**
 * TimeSeriesMemstore with a NullChunkSink (no chunks persisted), and in-memory MetaStore.
 * Not what you want for production, but good for getting started and running tests.
 */
class TimeSeriesNullStoreFactory(config: Config, scheduler: Scheduler) extends StoreFactory {
  implicit val sched = scheduler
  val metaStore = SingleJvmInMemoryStore.metaStore
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, metaStore)
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}
