package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.config.Config

import filodb.core.metadata.MetaStore
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.{MemTable, Scheduler, FlushPolicy, DefaultReprojector}

/**
 * A trait to make setup of the CoordinatorActor stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  val system: ActorSystem
  // The global configuration object
  val config: Config

  // TODO: Allow for a configurable thread pool for the futures, don't just use the global one
  // and strongly consider using a BlockingQueue with the ThreadPoolExecutor with limited capacity
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  // These should be implemented as lazy val's
  val memTable: MemTable
  val flushPolicy: FlushPolicy
  val columnStore: ColumnStore
  val metaStore: MetaStore
  lazy val reprojector = new DefaultReprojector(columnStore)
  lazy val scheduler = new Scheduler(memTable,
                                     reprojector,
                                     flushPolicy,
                                     config.getInt("scheduler-max-tasks"))

  lazy val coordinatorActor =
    system.actorOf(CoordinatorActor.props(memTable, metaStore, scheduler,
                                          config.getConfig("coordinator")),
                   "coordinator")

}
