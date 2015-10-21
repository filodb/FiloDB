package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.config.Config

import filodb.core.columnstore.ColumnStore
import filodb.core.FutureUtils
import filodb.core.metadata.MetaStore
import filodb.core.reprojector._

/**
 * A trait to make setup of the [[NodeCoordinatorActor]] stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  def system: ActorSystem
  // The global configuration object
  def config: Config

  implicit lazy val ec = FutureUtils.getBoundedExecContext(config.getInt("max-reprojection-futures"),
                                                      "filodb.core",
                                                      config.getInt("core-futures-pool-size"))

  // These should be implemented as lazy val's, though tests might want to reset them
  val columnStore: ColumnStore
  val metaStore: MetaStore
  lazy val reprojector = new DefaultReprojector(columnStore)

  // TODO: consider having a root actor supervising everything
  lazy val coordinatorActor =
    system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config),
                   "coordinator")
}

/**
 * A CoordinatorSetup with default memtable initialized from config
 */
trait DefaultCoordinatorSetup extends CoordinatorSetup