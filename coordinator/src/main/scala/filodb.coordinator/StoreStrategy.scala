package filodb.coordinator

import scala.concurrent.ExecutionContext

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.core.store._

/** Strategies from configuration or type. */
abstract class StoreStrategy
object StoreStrategy {
  case object InMemory extends StoreStrategy
  final case class Configured(name: String) extends StoreStrategy
}

/**
  * A StoreFactory creates instances of ColumnStore and MetaStore one time.  The columnStore and metaStore
  * methods should return that created instance every time, not create a new instance.  The implementation
  * should be a class that is passed a single parameter, the CoordinatorSetup instance with config,
  * ExecutionContext, etc.
  */
trait StoreFactory {
  def columnStore: ColumnStore with ColumnStoreScanner
  def metaStore: MetaStore
}

object StoreFactory extends Instance with StrictLogging {

  /** Initializes columnStore and metaStore using the factory setting from config. */
  def apply(settings: FilodbSettings, ec: ExecutionContext, readEc: ExecutionContext): StoreFactory =

    settings.StorageStrategy match {
      case StoreStrategy.InMemory =>
        new InMemoryStoreFactory(ec, readEc)

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
            new InMemoryStoreFactory(ec, readEc)
          }.getOrElse(new InMemoryStoreFactory(ec, readEc))
    }
}

class InMemoryStoreFactory(executionContext: ExecutionContext,
                           readExecutionContext: ExecutionContext) extends StoreFactory {
  implicit val ec = executionContext
  val columnStore = new InMemoryColumnStore(readExecutionContext)
  val metaStore = SingleJvmInMemoryStore.metaStore
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}
