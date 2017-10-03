package filodb.coordinator

import scala.concurrent.ExecutionContext

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.core.store._
import filodb.core.memstore.{MemStore, TimeSeriesMemStore}

/** Strategies from configuration or type. */
sealed trait StoreStrategy
object StoreStrategy {
  case object InMemory extends StoreStrategy
  final case class Configured(fqcn: String) extends StoreStrategy
}

/**
  * A StoreFactory creates instances of MemStore and MetaStore one time.  The memStore and metaStore
  * methods should return that created instance every time, not create a new instance.  The implementation
  * should be a class that is passed three params, the FilodbSettings, then two ExecutionContexts
  */
trait StoreFactory {
  def memStore: MemStore
  def metaStore: MetaStore
}

object StoreFactory extends Instance with StrictLogging {

  /** Initializes columnStore and metaStore using the factory setting from config. */
  def apply(settings: FilodbSettings, ec: ExecutionContext, readEc: ExecutionContext): StoreFactory =

    settings.StorageStrategy match {
      case StoreStrategy.InMemory =>
        new InMemoryStoreFactory(settings.config, ec)

      case StoreStrategy.Configured(fqcn) =>
        createClass(fqcn)
          .map { c =>
            val args = Seq(
              (classOf[Config] -> settings.config),
              (classOf[ExecutionContext] -> ec),
              (classOf[ExecutionContext] -> readEc))

            createInstance[StoreFactory](c, args).get
          }
          .recover { case e: ClassNotFoundException =>
            logger.error(s"Configured StoreFactory class '$fqcn' not found on classpath. Falling back to in-memory.", e)
            new InMemoryStoreFactory(settings.config, ec)
          }.getOrElse(new InMemoryStoreFactory(settings.config, ec))
    }
}

class InMemoryStoreFactory(config: Config, executionContext: ExecutionContext) extends StoreFactory {
  implicit val ec = executionContext
  val memStore = new TimeSeriesMemStore(config)
  val metaStore = SingleJvmInMemoryStore.metaStore
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}
