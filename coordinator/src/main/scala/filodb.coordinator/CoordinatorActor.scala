package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, MetaStore}
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.{MemTable, Scheduler}

/**
 * The CoordinatorActor is the common API entry point for all FiloDB ingestion and metadata operations.
 * It is a singleton - there should be exactly one such actor per node/JVM process.
 * It is responsible for:
 *  - Acting as the gateway to the MemTable, Scheduler, and MetaStore for local and remote actors
 *  - Supervising any other actors, such as regular scheduler heartbeats
 *  - Maintaining MemTable backpressure on clients to prevent OOM
 *
 * It is called by local (eg HTTP) as well as remote (eg Spark ETL) processes.
 */
object CoordinatorActor {
  /**
   * Creates a new dataset with columns and a default projection.
   */
  case class CreateDataset(dataset: Dataset, columns: Seq[Column])

  case object DatasetCreated extends Response
  case class DatasetError(msg: String) extends ErrorResponse

  /**
   * Sets up ingestion for a given dataset, version, and schema of columns.
   * The dataset and columns must have been previously defined.
   *
   * @returns BadSchema if the partition column is unsupported, sort column invalid, etc.
   */
  case class SetupIngestion(dataset: String, schema: Seq[String], version: Int)

  case object IngestionReady extends Response
  case object UnknownDataset extends ErrorResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse
  case object AlreadySetup extends ErrorResponse
  case object BadSchema extends ErrorResponse

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @param seqNo the sequence number to be returned for acknowledging the entire set of rows
   */
  case class IngestRows(dataset: String, version: Int, rows: Seq[RowReader], seqNo: Long)

  case class Ack(seqNo: Long) extends Response

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   */
  case class Flush(dataset: String, version: Int)

  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props(memTable: MemTable,
            metaStore: MetaStore,
            scheduler: Scheduler,
            columnStore: ColumnStore,
            config: Config): Props =
    Props(classOf[CoordinatorActor], memTable, metaStore, scheduler, columnStore, config)
}

/**
 * ==Configuration==
 * {{{
 *   {
 *     memtable-retry-interval = 10 s
 *     scheduler-interval = 1 s
 *     scheduler-reporting-interval = 30s
 *   }
 * }}}
 */
class CoordinatorActor(memTable: MemTable,
                       metaStore: MetaStore,
                       scheduler: Scheduler,
                       columnStore: ColumnStore,
                       config: Config) extends BaseActor {
  import CoordinatorActor._
  import context.dispatcher

  val memtablePushback = config.as[FiniteDuration]("memtable-retry-interval")
  val schedulerInterval = config.as[FiniteDuration]("scheduler-interval")
  val reportingInterval = config.as[FiniteDuration]("scheduler-reporting-interval")

  val schedulerActor = context.actorOf(SchedulerActor.props(scheduler), "scheduler")
  context.system.scheduler.schedule(schedulerInterval, schedulerInterval,
                                    schedulerActor, SchedulerActor.RunOnce)
  context.system.scheduler.schedule(reportingInterval, reportingInterval,
                                    schedulerActor, SchedulerActor.ReportStats)

  private def verifySchema(originator: ActorRef, dataset: String, version: Int, columns: Seq[String]):
      Future[Option[Column.Schema]] = {
    metaStore.getSchema(dataset, version).map { schema =>
      val undefinedCols = invalidColumns(columns, schema)
      if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
        originator ! UndefinedColumns(undefinedCols.toSeq)
        None
      } else {
        Some(schema)
      }
    }
  }

  private def createDataset(originator: ActorRef, datasetObj: Dataset, columns: Seq[Column]) = {
    if (datasetObj.projections.isEmpty) {
      originator ! DatasetError(s"There must be at least one projection in dataset $datasetObj")
    } else {
      (for { resp1 <- metaStore.newDataset(datasetObj)
             resp2 <- Future.sequence(columns.map(metaStore.newColumn(_)))
             resp3 <- columnStore.initializeProjection(datasetObj.projections.head) }
      yield {
        originator ! DatasetCreated
      }).recover {
        case e: StorageEngineException => originator ! e
      }
    }
  }

  private def setupIngestion(originator: ActorRef, dataset: String, columns: Seq[String], version: Int):
      Unit = {
    if (memTable.getIngestionSetup(dataset, version).isDefined) {
      logger.info(s"Dataset $dataset / $version ingestion already set up, nothing to do...")
      originator ! IngestionReady
      return
    }
    (for { datasetObj <- metaStore.getDataset(dataset)
           schema <- verifySchema(originator, dataset, version, columns) if schema.isDefined }
    yield {
      val columnSeq = columns.map(schema.get(_))
      memTable.setupIngestion(datasetObj, columnSeq, version) match {
        case MemTable.SetupDone    => originator ! IngestionReady
        case MemTable.AlreadySetup => originator ! AlreadySetup
        case MemTable.BadSchema    => originator ! BadSchema
      }
    }).recover {
      case NotFoundError(what) => originator ! UnknownDataset
      case t: Throwable        => originator ! MetadataException(t)
    }
  }

  def receive: Receive = {
    case CreateDataset(datasetObj, columns) =>
      createDataset(sender, datasetObj, columns)

    case SetupIngestion(dataset, columns, version) =>
      setupIngestion(sender, dataset, columns, version)

    case ingestCmd @ IngestRows(dataset, version, rows, seqNo) =>
      // Check if we are over limit or under memory
      // Ingest rows into the memtable
      memTable.ingestRows(dataset, version, rows) match {
        case MemTable.NoSuchDatasetVersion => sender ! UnknownDataset
        case MemTable.Ingested             => sender ! Ack(seqNo)
        case MemTable.PleaseWait           =>
          logger.debug(s"MemTable full, retrying in $memtablePushback...")
          context.system.scheduler.scheduleOnce(memtablePushback, self, ingestCmd)
      }

    case flushCmd @ Flush(dataset, version) =>
      schedulerActor.forward(flushCmd)
  }
}