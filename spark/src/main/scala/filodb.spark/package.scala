package filodb

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame, Row}
import org.apache.spark.sql.types.StructType
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.coordinator.{NodeCoordinatorActor, RowSource, DatasetCoordinatorActor}
import org.apache.spark.sql.hive.filodb.MetaStoreSync

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
   */
  case class IngestionOptions(version: Int = 0,
                              chunkSize: Option[Int] = None,
                              writeTimeout: FiniteDuration = DefaultWriteTimeout,
                              flushAfterInsert: Boolean = true)

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

  import NodeCoordinatorActor._
  import filodb.spark.FiloRelation._
  import FiloSetup.metaStore
  import RowSource._

  val sparkLogger = logger

  private[spark] def ingestRddRows(coordinatorActor: ActorRef,
                                   dataset: DatasetRef,
                                   columns: Seq[String],
                                   version: Int,
                                   rows: Iterator[Row],
                                   writeTimeout: FiniteDuration,
                                   partitionIndex: Int): Unit = {
    val props = RddRowSourceActor.props(rows, columns, dataset, version, coordinatorActor)
    val rddRowActor = FiloSetup.system.actorOf(props, s"${dataset}_${version}_$partitionIndex")
    actorAsk(rddRowActor, Start, writeTimeout) {
      case AllDone =>
      case SetupError(UnknownDataset) => throw DatasetNotFound(dataset.dataset)
      case SetupError(BadSchema(reason)) => throw BadSchemaError(reason)
      case SetupError(other)          => throw new RuntimeException(other.toString)
    }
  }

  /**
   * Syncs FiloDB datasets into Hive Metastore.
   * Usually does not need to be called manually, unless you did not use the right HiveContext/Spark
   * to create FiloDB tables.
   */
  def syncToHive(sqlContext: SQLContext): Unit = {
    val config = Option(FiloSetup.config).getOrElse {
      FiloSetup.init(sqlContext.sparkContext)
      FiloSetup.config
    }
    if (config.hasPath("hive.database-name")) {
      MetaStoreSync.getHiveContext(sqlContext).foreach { hiveContext =>
        MetaStoreSync.syncFiloTables(config.getString("hive.database-name"),
                                     FiloSetup.metaStore,
                                     hiveContext)
      }
    }
  }

  private[spark] def runCommands[B](cmds: Set[Future[Response]]): Unit = {
    val responseSet = Await.result(Future.sequence(cmds), 5 seconds)
    if (!responseSet.forall(_ == Success)) throw new RuntimeException(s"Some commands failed: $responseSet")
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
    val schema = parse(metaStore.getSchema(dataset, version)) { schema =>
      logger.info(s"Read schema for dataset $dataset = $schema")
      schema
    }

    // Translate DF schema to columns, create new ones if needed
    val dfSchema = dfColumns.map { col => col.name -> col }.toMap
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
      val addMissingCols = missingCols.map { colName =>
        val newCol = dfSchema(colName).copy(dataset = dataset.dataset, version = version)
        metaStore.newColumn(newCol, dataset)
      }
      runCommands(addMissingCols)
    }
  }

  // Checks for schema errors via RichProjection.make, and returns created Dataset object
  private[spark] def makeAndVerifyDataset(datasetRef: DatasetRef,
                                          rowKeys: Seq[String],
                                          segmentKey: String,
                                          partitionKeys: Seq[String],
                                          chunkSize: Option[Int],
                                          dfColumns: Seq[Column]): Dataset = {
    val options = Dataset.DefaultOptions
    val options2 = chunkSize.map { newSize => options.copy(chunkSize = newSize) }.getOrElse(options)
    val dataset = Dataset(datasetRef, rowKeys, segmentKey, partitionKeys).copy(options = options2)

    // validate against schema.  Checks key names, computed columns, etc.
    RichProjection.make(dataset, dfColumns).recover {
      case err: RichProjection.BadSchema => throw BadSchemaError(err.toString)
    }

    dataset
  }

  // This doesn't create columns, because that's in checkAndAddColumns.
  private[spark] def createNewDataset(dataset: Dataset): Unit = {
    logger.info(s"Creating dataset ${dataset.name}...")
    actorAsk(FiloSetup.coordinatorActor, CreateDataset(dataset, Nil)) {
      case DatasetCreated =>
        logger.info(s"Dataset ${dataset.name} created successfully...")
      case DatasetError(errMsg) =>
        throw new RuntimeException(s"Error creating dataset: $errMsg")
    }
  }

  private[spark] def truncateDataset(dataset: Dataset, version: Int): Unit = {
    logger.info(s"Truncating dataset ${dataset.name}")
    actorAsk(FiloSetup.coordinatorActor,
             TruncateProjection(dataset.projections.head, version), 1.minute) {
      case ProjectionTruncated => logger.info(s"Truncation of ${dataset.name} finished")
      case DatasetError(msg) => throw NotFoundError(s"$msg - (${dataset.name}, ${version})")
    }
  }

  private[spark] def deleteDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Deleting dataset $dataset")
    parse(FiloSetup.metaStore.deleteDataset(dataset)) { resp => resp }
  }

  implicit def sqlToFiloContext(sql: SQLContext): FiloContext = new FiloContext(sql)
}