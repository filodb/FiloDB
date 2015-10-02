package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, MetaStore, Projection}
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
  case class BadSchema(message: String) extends ErrorResponse

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
   * @returns SchedulerActor.Flushed
   */
  case class Flush(dataset: String, version: Int)

  /**
   * Truncates all data from a projection of a dataset.  Waits for any pending flushes from said
   * dataset to finish first, and also clears the columnStore cache for that dataset.
   */
  case class TruncateProjection(projection: Projection)
  case object ProjectionTruncated

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
 *     scheduler-interval = 1 s
 *     scheduler-reporting = false
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

  val schedulerInterval = config.as[FiniteDuration]("scheduler-interval")
  val turnOnReporting   = config.as[Option[Boolean]]("scheduler-reporting").getOrElse(false)
  val reportingInterval = config.as[FiniteDuration]("scheduler-reporting-interval")

  val schedulerActor = context.actorOf(SchedulerActor.props(scheduler), "scheduler")
  context.system.scheduler.schedule(schedulerInterval, schedulerInterval,
                                    schedulerActor, SchedulerActor.RunOnce)
  if (turnOnReporting) {
    context.system.scheduler.schedule(reportingInterval, reportingInterval,
                                      schedulerActor, SchedulerActor.ReportStats)
  }

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
    (for { datasetObj <- metaStore.getDataset(dataset)
           schema <- verifySchema(originator, dataset, version, columns) if schema.isDefined }
    yield {
      val columnSeq = columns.map(schema.get(_))
      memTable.setupIngestion(datasetObj, columnSeq, version) match {
        case MemTable.SetupDone    => originator ! IngestionReady
        // If the table is already set up, that's fine!
        case MemTable.AlreadySetup => originator ! IngestionReady
        case MemTable.BadSchema(msg) => originator ! BadSchema(msg)
      }
    }).recover {
      case NotFoundError(what) => originator ! UnknownDataset
      case t: Throwable        => originator ! MetadataException(t)
    }
  }

  private def truncateProjection(originator: ActorRef, projection: Projection): Unit = {
    for { reprojResult <- scheduler.waitForReprojection(projection.dataset)
          resp <- columnStore.clearProjectionData(projection) }
    { originator ! ProjectionTruncated }
  }

  def receive: Receive = {
    case CreateDataset(datasetObj, columns) =>
      createDataset(sender, datasetObj, columns)

    case SetupIngestion(dataset, columns, version) =>
      setupIngestion(sender, dataset, columns, version)

    case ingestCmd @ IngestRows(dataset, version, rows, seqNo) =>
      // Ingest rows into the memtable
      memTable.ingestRows(dataset, version, rows) match {
        case MemTable.NoSuchDatasetVersion => sender ! UnknownDataset
        case MemTable.Ingested             => sender ! Ack(seqNo)
        case MemTable.PleaseWait           =>
          logger.info(s"MemTable full or low on memory, try rows again later...")
      }

    case flushCmd @ Flush(dataset, version) =>
      schedulerActor.forward(flushCmd)

    case TruncateProjection(projection) =>
      truncateProjection(sender, projection)
  }
}