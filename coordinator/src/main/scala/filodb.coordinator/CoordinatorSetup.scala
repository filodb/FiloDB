package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.config.Config

import filodb.core.columnstore.ColumnStore
import filodb.core.FutureUtils
import filodb.core.metadata.MetaStore
import filodb.core.reprojector._

/**
 * A trait to make setup of the NodeCoordinatorActor stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  val system: ActorSystem
  // The global configuration object
  def config: Config

  implicit lazy val ec = FutureUtils.getBoundedExecContext(config.getInt("max-reprojection-futures"),
                                                      "filodb.core",
                                                      config.getInt("core-futures-pool-size"))

  // These should be implemented as lazy val's, though tests might want to reset them
  val memTable: MemTable
  val columnStore: ColumnStore
  val metaStore: MetaStore
  lazy val reprojector = new DefaultReprojector(columnStore)

  lazy val coordinatorActor =
    system.actorOf(NodeCoordinatorActor.props(memTable, metaStore, reprojector, columnStore,
                                              config),
                   "coordinator")

  def clearState(): Unit = {
    memTable.clearAllData()
  }
}

/**
 * A CoordinatorSetup with default memtable initialized from config
 */
trait DefaultCoordinatorSetup extends CoordinatorSetup {
  lazy val memTable = new MapDBMemTable(config)
}