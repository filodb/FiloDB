package filodb.standalone

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive

import filodb.coordinator.{BaseActor, RowSource, IngestionCommands}

object IngestionActor {
  def props(rowSource: ActorRef): Props = Props(classOf[IngestionActor], rowSource)
}

/**
 * Handles messages and errors from the rowSource.
 * TODO: make sure errors cause the rowSource to be restarted
 */
class IngestionActor(rowSource: ActorRef) extends BaseActor {
  import RowSource._
  import IngestionCommands._

  rowSource ! Start

  def receive: Receive = LoggingReceive {
      case AllDone =>
        logger.info(s"Received AllDone from RowSource.  Initiating shutdown sequence...")
        FiloServer.shutdownAndExit(0)
      case SetupError(UnknownDataset)    =>
        logger.error("Unable to set up dataset ingestion")
        FiloServer.shutdownAndExit(1)
      case SetupError(BadSchema(reason)) =>
        logger.error(s"Bad schema setting up ingestion: $reason")
        FiloServer.shutdownAndExit(1)
      case SetupError(other)             =>
        logger.error(s"Other ingestion setup error: $other")
        FiloServer.shutdownAndExit(1)
      // case IngestionErr(errString, None) => throw new RuntimeException(errString)
      // case IngestionErr(errString, Some(e)) => throw new RuntimeException(errString, e)
  }
}