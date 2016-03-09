package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext

import filodb.core.store.{ColumnStore, MetaStore}
import filodb.core.FutureUtils
import filodb.core.reprojector._

/**
 * A trait to make setup of the [[NodeCoordinatorActor]] stack a bit easier.
 * Mixed in for tests as well as the main FiloDB app and anywhere else the stack needs to be spun up.
 */
trait CoordinatorSetup {
  def system: ActorSystem

  // The global Filo configuration object.  Should be ConfigFactory.load.getConfig("filodb")
  def config: Config

  lazy val threadPool = FutureUtils.getBoundedTPE(config.getInt("core-futures-queue-length"),
                                                  "filodb.core",
                                                  config.getInt("core-futures-pool-size"),
                                                  config.getInt("core-futures-max-pool-size"))

  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  // A separate ExecutionContext can optionally be used for reads, to control read task queue length
  // separately perhaps.  I have found this is not really necessary.  This was originally created to
  // help decongest heavy write workloads, but a better design has been the throttling of reprojection
  // by limiting number of segments flushed at once (see the use of foldLeftSequentially in Reprojector).
  lazy val readEc = ec

  // These should be implemented as lazy val's, though tests might want to reset them
  val columnStore: ColumnStore
  val metaStore: MetaStore
  lazy val reprojector = new DefaultReprojector(config, columnStore)

  // TODO: consider having a root actor supervising everything
  lazy val coordinatorActor =
    system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config),
                   "coordinator")

  def shutdown(): Unit = {
    system.shutdown()
    columnStore.shutdown()
    metaStore.shutdown()
    // Important: shut down executioncontext as well
    threadPool.shutdown()
  }
}
