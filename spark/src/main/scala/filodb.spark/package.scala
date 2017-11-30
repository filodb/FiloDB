package filodb

import java.sql.Timestamp

import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.hive.filodb.MetaStoreSync
import org.apache.spark.sql.types.StructType
import org.scalactic._

import filodb.coordinator._
import filodb.coordinator.client.ClientException
import filodb.core._
import filodb.core.memstore.{IngestRecord, IngestRouting}
import filodb.core.metadata.{Column, Dataset, DatasetOptions}
import filodb.memory.format.RowReader

package spark {
  case class DatasetNotFound(dataset: String) extends Exception(s"Dataset $dataset not found")
  // For each mismatch: the column name, DataFrame type, and existing column type
  case class ColumnTypeMismatch(mismatches: Set[(String, String, String)]) extends
    Exception(s"Mismatches:\n${mismatches.toList.mkString("\n")}")
  case class BadSchemaError(reason: String) extends Exception(reason)

  /**
   * Options for calling saveAsFilo
   * @param chunkSize an optionally different chunkSize to set new dataset to use
   * @param writeTimeout Maximum time to wait for write of each partition to complete
   * @param flushAfterInsert if true, ensure all data is flushed in memtables at end of ingestion
   * @param resetSchema if true, allows dataset schema (eg partition keys) to be reset when overwriting
   *          an existing dataset
   */
  case class IngestionOptions(chunkSize: Option[Int] = None,
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

  import IngestionStream._
  import filodb.coordinator.client.Client.{actorAsk, parse}
  import FiloDriver.metaStore

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
                                   dataset: Dataset,
                                   routing: IngestRouting,
                                   rows: Iterator[Row],
                                   writeTimeout: FiniteDuration,
                                   partitionIndex: Int): Unit = {
    val mapper = actorAsk(clusterActor, NodeClusterActor.GetShardMap(dataset.ref)) {
      case newMap: ShardMapper => newMap
    }

    val stream = new IngestionStream {
      def get: Observable[Seq[IngestRecord]] = {
        val recordSeqIt = rows.grouped(1000).zipWithIndex.map { case (rows, idx) =>
          if (idx % 20 == 0) logger.info(s"Ingesting batch starting at row ${idx * 1000}")
          rows.map { row => IngestRecord(routing, RddRowReader(row), idx) }
        }
        Observable.fromIterator(recordSeqIt)
      }
      def teardown(): Unit = {}
    }

    // NOTE: might need to force scheduler to run on current thread due to bug with Spark ThreadLocal
    // during shuffles.
    stream.routeToShards(mapper, dataset, getProtocolActor(clusterActor, dataset.ref))(FiloExecutor.ec)
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

  private[spark] def dfToFiloColumns(df: DataFrame): Seq[String] = dfToFiloColumns(df.schema)

  // Creates name:type strings as needed for Dataset creation
  private[spark] def dfToFiloColumns(schema: StructType): Seq[String] =
    schema.map { f => s"${f.name}:${sqlTypeToTypeName(f.dataType)}" }

  private[spark] def checkColumns(dfColumns: Seq[String],
                                  dataset: Dataset): Unit = {
    // Check DF schema against dataset columns, get column IDs back
    val nameToType = dfColumns.map { nameAndType =>
      val parts = nameAndType.split(':')
      parts(0) -> parts(1)
    }.toMap
    logger.info(s"Columns from Dataframe Schema: $nameToType")
    val colIDs = dataset.colIDs(nameToType.keys.toSeq: _*) match {
      case Good(colIDs) => colIDs
      case Bad(missing) => throw BadSchemaError(s"Columns $missing not in dataset ${dataset.ref}")
    }

    // Type-check matching columns
    val matchingTypeErrs = colIDs.map(id => dataset.columnFromID(id)).collect {
      case c: Column if nameToType(c.name) != c.columnType.typeName =>
        (c.name, nameToType(c.name), c.columnType.typeName)
    }
    if (matchingTypeErrs.nonEmpty) throw ColumnTypeMismatch(matchingTypeErrs.toSet)
  }

  private[spark] def flushAndLog(dataset: DatasetRef): Unit = {
    try {
      val nodesFlushed = FiloDriver.client.flushCompletely(dataset, flushTimeout)
      sparkLogger.info(s"Flush completed on $nodesFlushed nodes for dataset $dataset")
    } catch {
      case ClientException(msg) =>
        sparkLogger.warn(s"Could not flush due to client exception $msg on dataset $dataset...")
      case e: Exception =>
        sparkLogger.warn(s"Exception from flushing nodes for $dataset", e)
    }
  }

  // Checks for schema errors, and returns created Dataset object
  private[spark] def makeAndVerifyDataset(datasetRef: DatasetRef,
                                          rowKeys: Seq[String],
                                          partitionColumns: Seq[String],
                                          chunkSize: Option[Int],
                                          dfColumns: Seq[String]): Dataset = {
    val options = DatasetOptions.DefaultOptions
    val options2 = chunkSize.map { newSize => options.copy(chunkSize = newSize) }.getOrElse(options)
    val partColNames = partitionColumns.map(_.split(':')(0)).toSet
    val dataColumns = dfColumns.filterNot(partColNames contains _.split(':')(0))
    sparkLogger.info(s"Creating dataset $datasetRef with partition columns $partitionColumns, " +
                     s"data columns $dataColumns, row keys $rowKeys")
    Dataset(datasetRef.dataset, partitionColumns, dataColumns, rowKeys).copy(options = options2)
  }

  private[spark] def createNewDataset(dataset: Dataset, database: Option[String]): Unit = {
    logger.info(s"Creating dataset ${dataset.name}...")
    // FiloDriver.client.createNewDataset(dataset, database, timeout = datasetOpTimeout)
    // TODO: fix: cannot serialize CreateDataset actor message
    parse(FiloDriver.metaStore.newDataset(dataset), datasetOpTimeout) { x => x }
  }

  implicit def sqlToFiloContext(sql: SQLContext): FiloContext = new FiloContext(sql)

  implicit def sessionToFiloContext(sess: SparkSession): FiloContext = new FiloContext(sess.sqlContext)
}