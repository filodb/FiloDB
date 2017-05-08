package filodb.standalone

import akka.actor.ActorSystem
import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.coordinator.client.LocalClient
import filodb.coordinator.{CoordinatorSetupWithFactory, RowSourceFactory}

/**
 * FiloServer starts a "standalone" FiloDB server which can ingest and support queries through the Akka
 * API.
 *
 * - Ingestion happens through a configurable RowSource, continuously
 * - The servers connect to each other setting up an Akka Cluster.  Seed nodes must be configured.
 * - LIMITATION: only ingestion from a single source/dataset is supported.  All nodes do ingestion.
 */
object FiloServer extends App with CoordinatorSetupWithFactory with StrictLogging {
  val config = systemConfig.getConfig("filodb")
  lazy val system = ActorSystem("filo-standalone", systemConfig)

  kamonInit()
  coordinatorActor
  Await.result(metaStore.initialize(), 60.seconds)

  // initialize Akka Cluster
  // TODO: revamp this for discovery-based seed node joining
  cluster    // rely on configuration-based seed node joining

  // get cluster actor
  lazy val clusterActor = singletonClusterActor("worker")

  // initialize rowsource and get it started
  val sourceFactoryClass = config.getString("standalone.rowsource-factory")
  val ctor = Class.forName(sourceFactoryClass).getConstructors.head
  val sourceFactory = ctor.newInstance().asInstanceOf[RowSourceFactory]

  // Get projection from RowSourceFactory
  val client = new LocalClient(coordinatorActor)
  val proj = sourceFactory.getProjection(this, client)
  val sourceActor = sourceFactory.create(this, proj, clusterActor)

  // NOTE/TODO: it seems a total waste to need a projection for ingestion, see if we can eliminate it.

  // Send IngestionSetup to coordinator
  val errors = client.setupIngestion(proj.datasetRef, proj.dataColumns.map(_.name), 0)
  if (errors.nonEmpty) {
    logger.error(s"Could not set up ingestion for ${proj.datasetRef}, errors: $errors")
    shutdownAndExit(1)
  }

  // Since this is a long-running process, and will want to recover from errors automatically,
  // we should wrap the RowSource error handling around another actor
  // This sends Start message to start ingestion
  val ingestionRef = system.actorOf(IngestionActor.props(sourceActor))

  // Send start message

  def shutdownAndExit(code: Int): Unit = {
    shutdown()
    sys.exit(code)
  }
}