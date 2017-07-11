package filodb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.ArrayBlockingQueue
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession, SaveMode}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.language.postfixOps

import org.apache.spark.sql.hive.filodb.MetaStoreSync

import filodb.coordinator.client.ClientException
import filodb.coordinator.{DatasetCommands, DatasetCoordinatorActor, IngestionCommands, RowSource}
import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}

package spark {
  case class DatasetNotFound(dataset: String) extends Exception(s"Dataset $dataset not found")
  // For each mismatch: the column name, DataFrame type, and existing column type
  case class ColumnTypeMismatch(mismatches: Set[(String, Column.ColumnType, Column.ColumnType)]) extends
    Exception(s"Mismatches:\n${mismatches.toList.mkString("\n")}")
  case class BadSchemaError(reason: String) extends Exception(reason)

  /**
   * Options for calling saveAsFilo
   * @param version the version number to write to
   * @param chunkSize an optionally different chunkSize to set new dataset to use
   * @param writeTimeout Maximum time to wait for write of each partition to complete
   * @param flushAfterInsert if true, ensure all data is flushed in memtables at end of ingestion
   * @param resetSchema if true, allows dataset schema (eg partition keys) to be reset when overwriting
   *          an existing dataset
   */
  case class IngestionOptions(version: Int = 0,
                              chunkSize: Option[Int] = None,
                              writeTimeout: FiniteDuration = DefaultWriteTimeout,
                              flushAfterInsert: Boolean = true,
                              resetSchema: Boolean = false)

}

/**
 * Provides base methods for reading from and writing to FiloDB tables/datasets.
 * Note that this is not the recommended DataFrame load/save API, please see DefaultSource.scala.
 * Configuration is done through setting SparkConf variables, like filodb.cassandra.keyspace
 * Here is how you could use these APIs
 *
 * {{{
 *   > import filodb.spark._
 *   > sqlContext.saveAsFiloDataset(myDF, "table1", rowKeys, partitionKeys, segmentKey, createDataset=true)
 *
 *   > sqlContext.filoDataset("table1")
 * }}}
 */
package object spark extends StrictLogging {
  val DefaultWriteTimeout = 999 minutes

  import IngestionCommands._
  import DatasetCommands._
  import FiloDriver.metaStore
  import RowSource._
  import filodb.coordinator.client.Client.{parse, actorAsk}

  val sparkLogger = logger

  val actorCounter = new java.util.concurrent.atomic.AtomicInteger

  lazy val datasetOpTimeout = FiloDriver.config.as[FiniteDuration]("spark.dataset-ops-timeout")
  lazy val flushTimeout     = FiloDriver.config.as[FiniteDuration]("spark.flush-timeout")

  private[spark] def ingestRddRows(clusterActor: ActorRef,
                                   projection: RichProjection,
                                   version: Int,
                                   rows: Iterator[Row],
                                   writeTimeout: FiniteDuration,
                                   partitionIndex: Int): Unit = {
    // Use a queue and read off of iterator in this, the Spark thread.  Due to the presence of ThreadLocals
    // it is not safe for us to read off of this iterator in another (ie Actor) thread
    val queue = new ArrayBlockingQueue[Seq[Row]](32)
    val props = RddRowSourceActor.props(queue, projection, version, clusterActor)
    val actorId = actorCounter.getAndIncrement()
    val ref = projection.datasetRef
    val rddRowActor = FiloExecutor.system.actorOf(props, s"${ref}_${version}_${partitionIndex}_${actorId}")
    implicit val timeout = Timeout(writeTimeout)
    val resp = rddRowActor ? Start
    val rowChunks = rows.grouped(1000)
    var i = 0
    while (rowChunks.hasNext && !resp.value.isDefined) {
      queue.put(rowChunks.next)
      if (i % 20 == 0) logger.info(s"Ingesting batch starting at row ${i * 1000}")
      i += 1
    }
    queue.put(Nil)    // Final marker that there are no more rows
    Await.result(resp, writeTimeout) match {
      case AllDone =>
      case SetupError(UnknownDataset)    => throw DatasetNotFound(ref.toString)
      case SetupError(BadSchema(reason)) => throw BadSchemaError(reason)
      case SetupError(other)             => throw new RuntimeException(other.toString)
      case IngestionErr(errString, None) => throw new RuntimeException(errString)
      case IngestionErr(errString, Some(e)) => throw new RuntimeException(errString, e)
    }
  }

  /**
   * Syncs FiloDB datasets into Hive Metastore.
   * Usually does not need to be called manually, unless you did not use the right HiveContext/Spark
   * to create FiloDB tables.
   */
  def syncToHive(sqlContext: SQLContext): Unit = {
    val config = FiloDriver.initAndGetConfig(sqlContext.sparkContext)
    if (config.hasPath("hive.database-name")) {
      MetaStoreSync.getSparkSession(sqlContext).foreach { sparkSession =>
        MetaStoreSync.syncFiloTables(config.getString("hive.database-name"),
                                     metaStore,
                                     sparkSession)
      }
    }
  }

  import filodb.spark.TypeConverters._

  private[spark] def dfToFiloColumns(df: DataFrame): Seq[DataColumn] = dfToFiloColumns(df.schema)

  private[spark] def dfToFiloColumns(schema: StructType): Seq[DataColumn] = {
    schema.map { f =>
      DataColumn(0, f.name, "", -1, sqlTypeToColType(f.dataType))
    }
  }

  private[spark] def checkAndAddColumns(dfColumns: Seq[DataColumn],
                                        dataset: DatasetRef,
                                        version: Int): Unit = {
    // Pull out existing dataset schema
    val schema = parse(metaStore.getSchema(dataset, version), datasetOpTimeout) { schema => schema }

    // Translate DF schema to columns, create new ones if needed
    val dfSchemaSeq = dfColumns.map { col => col.name -> col }
    logger.info(s"Columns from Dataframe Schema: ${dfSchemaSeq.map(_._2).zipWithIndex}")
    val dfSchema = dfSchemaSeq.toMap
    val matchingCols = dfSchema.keySet.intersect(schema.keySet)
    val missingCols = dfSchema.keySet -- schema.keySet
    logger.info(s"Matching columns - $matchingCols\nMissing columns - $missingCols")

    // Type-check matching columns
    val matchingTypeErrs = matchingCols.collect {
      case colName: String if dfSchema(colName).columnType != schema(colName).columnType =>
        (colName, dfSchema(colName).columnType, schema(colName).columnType)
    }
    if (matchingTypeErrs.nonEmpty) throw ColumnTypeMismatch(matchingTypeErrs)

    if (missingCols.nonEmpty) {
      val newCols = missingCols.map(dfSchema(_).copy(dataset = dataset.dataset, version = version))
      parse(metaStore.newColumns(newCols.toSeq, dataset), datasetOpTimeout) { resp =>
        if (resp != Success) throw new RuntimeException(s"Error $resp creating new columns $newCols")
      }
    }
  }

  private[spark] def flushAndLog(dataset: DatasetRef, version: Int): Unit = {
    try {
      val nodesFlushed = FiloDriver.client.flushCompletely(dataset, version, flushTimeout)
      sparkLogger.info(s"Flush completed on $nodesFlushed nodes for dataset $dataset")
    } catch {
      case ClientException(msg) =>
        sparkLogger.warn(s"Could not flush due to client exception $msg on dataset $dataset...")
      case e: Exception =>
        sparkLogger.warn(s"Exception from flushing nodes for $dataset/$version", e)
    }
  }

  // Checks for schema errors via RichProjection.make, and returns created Dataset object
  private[spark] def makeAndVerifyDataset(datasetRef: DatasetRef,
                                          rowKeys: Seq[String],
                                          partitionKeys: Seq[String],
                                          chunkSize: Option[Int],
                                          dfColumns: Seq[Column]): Dataset = {
    val options = Dataset.DefaultOptions
    val options2 = chunkSize.map { newSize => options.copy(chunkSize = newSize) }.getOrElse(options)
    val dataset = Dataset(datasetRef, rowKeys, partitionKeys).copy(options = options2)

    // validate against schema.  Checks key names, computed columns, etc.
    RichProjection.make(dataset, dfColumns).recover {
      case err: RichProjection.BadSchema => throw BadSchemaError(err.toString)
    }

    dataset
  }

  // This doesn't create columns, because that's in checkAndAddColumns.
  private[spark] def createNewDataset(dataset: Dataset): Unit = {
    logger.info(s"Creating dataset ${dataset.name}...")
    actorAsk(FiloDriver.coordinatorActor, CreateDataset(dataset, Nil), datasetOpTimeout) {
      case DatasetCreated =>
        logger.info(s"Dataset ${dataset.name} created successfully...")
      case DatasetError(errMsg) =>
        throw new RuntimeException(s"Error creating dataset: $errMsg")
    }
  }

  private[spark] def deleteDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Deleting dataset $dataset")
    FiloDriver.client.deleteDataset(dataset, datasetOpTimeout)
  }

  implicit def sqlToFiloContext(sql: SQLContext): FiloContext = new FiloContext(sql)

  implicit def sessionToFiloContext(sess: SparkSession): FiloContext = new FiloContext(sess.sqlContext)
}