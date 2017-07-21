package filodb

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.sql.Timestamp
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession, SaveMode}
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.apache.spark.sql.hive.filodb.MetaStoreSync

import filodb.coordinator.client.ClientException
import filodb.coordinator._
import filodb.core._
import filodb.core.memstore.IngestRecord
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

  case class RddRowReader(row: Row) extends RowReader {
    def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
    def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
    def getInt(columnNo: Int): Int = row.getInt(columnNo)
    def getLong(columnNo: Int): Long =
      try { row.getLong(columnNo) }
      catch {
        case e: ClassCastException => row.getAs[Timestamp](columnNo).getTime
      }
    def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
    def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
    def getString(columnNo: Int): String = row.getString(columnNo)
    def getAny(columnNo: Int): Any = row.get(columnNo)
  }
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
  import IngestionStream._
  import NodeClusterActor.{SubscribeShardUpdates, ShardMapUpdate}
  import filodb.coordinator.client.Client.{parse, actorAsk}

  val sparkLogger = logger

  val actorCounter = new java.util.concurrent.atomic.AtomicInteger

  lazy val datasetOpTimeout = FiloDriver.config.as[FiniteDuration]("spark.dataset-ops-timeout")
  lazy val flushTimeout     = FiloDriver.config.as[FiniteDuration]("spark.flush-timeout")

  private val protocolActors = new HashMap[DatasetRef, ActorRef]

  private[spark] def getProtocolActor(clusterActor: ActorRef, ref: DatasetRef): ActorRef = synchronized {
    protocolActors.getOrElseUpdate(ref, {
      val actorId = actorCounter.getAndIncrement()
      FiloExecutor.system.actorOf(IngestProtocol.props(clusterActor, ref),
                                  s"ingestProtocol_${ref}_$actorId")
    })
  }

  /**
   * Sends rows from this Spark partition to the right FiloDB nodes using IngestionProtocol.
   * Might not be terribly efficient.
   * Uses a shared IngestionProtocol for better efficiency amongst all partitions.
   */
  private[spark] def ingestRddRows(clusterActor: ActorRef,
                                   projection: RichProjection,
                                   version: Int,
                                   rows: Iterator[Row],
                                   writeTimeout: FiniteDuration,
                                   partitionIndex: Int): Unit = {
    val ref = projection.datasetRef
    val mapper = actorAsk(clusterActor, SubscribeShardUpdates(ref)) {
      case ShardMapUpdate(_, newMap) => newMap
    }

    val stream = new IngestionStream {
      def get: Observable[Seq[IngestRecord]] = {
        val recordSeqIt = rows.grouped(1000).zipWithIndex.map { case (rows, idx) =>
          if (idx % 20 == 0) logger.info(s"Ingesting batch starting at row ${idx * 1000}")
          rows.map { row => IngestRecord(projection, RddRowReader(row), idx) }
        }
        Observable.fromIterator(recordSeqIt)
      }
      def teardown(): Unit = {}
    }

    // NOTE: might need to force scheduler to run on current thread due to bug with Spark ThreadLocal
    // during shuffles.
    stream.routeToShards(mapper, projection, getProtocolActor(clusterActor, ref))(FiloExecutor.ec)
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
    FiloDriver.client.createNewDataset(dataset, Nil, timeout = datasetOpTimeout)
  }

  private[spark] def deleteDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Deleting dataset $dataset")
    FiloDriver.client.deleteDataset(dataset, datasetOpTimeout)
  }

  implicit def sqlToFiloContext(sql: SQLContext): FiloContext = new FiloContext(sql)

  implicit def sessionToFiloContext(sess: SparkSession): FiloContext = new FiloContext(sess.sqlContext)
}