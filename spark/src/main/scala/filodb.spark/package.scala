package filodb

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame, Row}
import org.apache.spark.sql.types.StructType
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
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

  private def ingestRddRows(coordinatorActor: ActorRef,
                            dataset: String,
                            columns: Seq[String],
                            version: Int,
                            rows: Iterator[Row],
                            writeTimeout: FiniteDuration,
                            partitionIndex: Int): Unit = {
    val props = RddRowSourceActor.props(rows, columns, dataset, version, coordinatorActor)
    val rddRowActor = FiloSetup.system.actorOf(props, s"${dataset}_${version}_$partitionIndex")
    actorAsk(rddRowActor, Start, writeTimeout) {
      case AllDone =>
      case SetupError(UnknownDataset) => throw DatasetNotFound(dataset)
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

  implicit class FiloContext(sqlContext: SQLContext) {

    /**
     * Creates a DataFrame from a FiloDB table.  Does no reading until a query is run, but it does
     * read the schema for the table.
     * @param dataset the name of the FiloDB table/dataset to read from
     * @param version the version number to read from
     * @param splitsPerNode the parallelism or number of splits per node
     */
    def filoDataset(dataset: String,
                    version: Int = 0,
                    splitsPerNode: Int = 4): DataFrame =
      sqlContext.baseRelationToDataFrame(FiloRelation(dataset, version, splitsPerNode)
                                                     (sqlContext))

    private def runCommands[B](cmds: Set[Future[Response]]): Unit = {
      val responseSet = Await.result(Future.sequence(cmds), 5 seconds)
      if (!responseSet.forall(_ == Success)) throw new RuntimeException(s"Some commands failed: $responseSet")
    }

    import filodb.spark.TypeConverters._

    private def dfToFiloColumns(df: DataFrame): Seq[DataColumn] = dfToFiloColumns(df.schema)

    private def dfToFiloColumns(schema: StructType): Seq[DataColumn] = {
      schema.map { f =>
        DataColumn(0, f.name, "", -1, sqlTypeToColType(f.dataType))
      }
    }

    private def checkAndAddColumns(dfColumns: Seq[DataColumn],
                                   dataset: String,
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
          val newCol = dfSchema(colName).copy(dataset = dataset, version = version)
          metaStore.newColumn(newCol)
        }
        runCommands(addMissingCols)
      }
    }

    // Checks for schema errors via RichProjection.make, and returns created Dataset object
    private def makeAndVerifyDataset(datasetName: String,
                                     rowKeys: Seq[String],
                                     segmentKey: String,
                                     partitionKeys: Seq[String],
                                     chunkSize: Option[Int],
                                     dfColumns: Seq[Column]): Dataset = {
      val options = Dataset.DefaultOptions
      val options2 = chunkSize.map { newSize => options.copy(chunkSize = newSize) }.getOrElse(options)
      val dataset = Dataset(datasetName, rowKeys, segmentKey, partitionKeys).copy(options = options2)

      // validate against schema.  Checks key names, computed columns, etc.
      RichProjection.make(dataset, dfColumns).recover {
        case err: RichProjection.BadSchema => throw BadSchemaError(err.toString)
      }

      dataset
    }

    // This doesn't create columns, because that's in checkAndAddColumns.
    private def createNewDataset(dataset: Dataset): Unit = {
      logger.info(s"Creating dataset ${dataset.name}...")
      actorAsk(FiloSetup.coordinatorActor, CreateDataset(dataset, Nil)) {
        case DatasetCreated =>
          logger.info(s"Dataset ${dataset.name} created successfully...")
        case DatasetError(errMsg) =>
          throw new RuntimeException(s"Error creating dataset: $errMsg")
      }
    }

    private def truncateDataset(dataset: Dataset, version: Int): Unit = {
      logger.info(s"Truncating dataset ${dataset.name}")
      actorAsk(FiloSetup.coordinatorActor,
               TruncateProjection(dataset.projections.head, version), 1.minute) {
        case ProjectionTruncated => logger.info(s"Truncation of ${dataset.name} finished")
        case DatasetError(msg) => throw NotFoundError(s"$msg - (${dataset.name}, ${version})")
      }
    }

    private def deleteDataset(dataset: String): Unit = {
      logger.info(s"Deleting dataset $dataset")
      parse(FiloSetup.metaStore.deleteDataset(dataset)) { resp => resp }
    }

    /**
     * Creates (or recreates) a FiloDB dataset with certain row, segment, partition keys.  Only creates the
     * dataset/projection definition and persists the dataset metadata; does not actually create the column
     * definitions (that is done by the insert step).  The exact behavior depends on the mode:
     *   Append  - creates the dataset if it doesn't exist
     *   Overwrite - creates the dataset, deleting the old definition first if needed
     *   ErrorIfExists - throws an error if the dataset already exists
     *
     * For the other paramter definitions, please see saveAsFiloDataset().
     */
    private[spark] def createOrUpdateDataset(schema: StructType,
                                             dataset: String,
                                             rowKeys: Seq[String],
                                             segmentKey: String,
                                             partitionKeys: Seq[String],
                                             chunkSize: Option[Int] = None,
                                             mode: SaveMode = SaveMode.Append): Unit = {
      FiloSetup.init(sqlContext.sparkContext)
      val partKeys = if (partitionKeys.nonEmpty) partitionKeys else Seq(Dataset.DefaultPartitionColumn)
      val dfColumns = dfToFiloColumns(schema)

      val datasetObj = try {
        Some(getDatasetObj(dataset))
      } catch {
        case e: NotFoundError => None
      }
      (datasetObj, mode) match {
        case (None, SaveMode.Append) | (None, SaveMode.Overwrite) | (None, SaveMode.ErrorIfExists) =>
          val ds = makeAndVerifyDataset(dataset, rowKeys, segmentKey, partKeys, chunkSize, dfColumns)
          createNewDataset(ds)
        case (Some(dsObj), SaveMode.ErrorIfExists) =>
          throw new RuntimeException(s"Dataset $dataset already exists!")
        case (Some(dsObj), SaveMode.Overwrite) =>
          val ds = makeAndVerifyDataset(dataset, rowKeys, segmentKey, partKeys, chunkSize, dfColumns)
          deleteDataset(dataset)
          createNewDataset(ds)
        case (_, _) =>
          logger.info(s"Dataset $dataset definition not changed")
      }
    }

    /**
     * Saves a DataFrame in a FiloDB Table
     * - Creates columns in FiloDB from DF schema if needed
     *
     * @param df the DataFrame to write to FiloDB
     * @param dataset the name of the FiloDB table/dataset to read from
     * @param rowKeys the name of the column(s) used as the row primary key within each partition.
     *                May be computed functions.
     * @param segmentKey the name of the column or computed function used to group rows into segments and
     *                   to sort the partition by.
     * @param partitionKeys column name(s) used for partition key.  If empty, then the default Dataset
     *                      partition key of `:string /0` (a constant) will be used.
     *
     *          Partitioning columns could be created using an expression on another column
     *          {{{
     *            val newDF = df.withColumn("partition", df("someCol") % 100)
     *          }}}
     *          or even UDFs:
     *          {{{
     *            val idHash = sqlContext.udf.register("hashCode", (s: String) => s.hashCode())
     *            val newDF = df.withColumn("partition", idHash(df("id")) % 100)
     *          }}}
     *
     *          However, note that the above methods will lead to a physical column being created, so
     *          use of computed columns is probably preferable.
     *
     * @param version the version number to write to
     * @param chunkSize an optionally different chunkSize to set new dataset to use
     * @param mode the Spark SaveMode - ErrorIfExists, Append, Overwrite, Ignore
     * @param writeTimeout Maximum time to wait for write of each partition to complete
     */
    def saveAsFilo(df: DataFrame,
                   dataset: String,
                   rowKeys: Seq[String],
                   segmentKey: String,
                   partitionKeys: Seq[String],
                   version: Int = 0,
                   chunkSize: Option[Int] = None,
                   mode: SaveMode = SaveMode.Append,
                   writeTimeout: FiniteDuration = DefaultWriteTimeout): Unit = {
      createOrUpdateDataset(df.schema, dataset, rowKeys, segmentKey, partitionKeys, chunkSize, mode)
      insertIntoFilo(df, dataset, version, mode == SaveMode.Overwrite, writeTimeout)
    }

    /**
     * Implements INSERT INTO into a Filo Dataset.  The dataset must already have been created.
     * Will check and add any extra columns from the DataFrame into the dataset, but column type
     * mismatches will result in an error.
     * @param overwrite if true, first truncate the dataset before writing
     */
    def insertIntoFilo(df: DataFrame,
                       dataset: String,
                       version: Int = 0,
                       overwrite: Boolean = false,
                       writeTimeout: FiniteDuration = DefaultWriteTimeout): Unit = {
      val filoConfig = FiloSetup.initAndGetConfig(sqlContext.sparkContext)
      val dfColumns = dfToFiloColumns(df)
      val columnNames = dfColumns.map(_.name)
      checkAndAddColumns(dfColumns, dataset, version)

      if (overwrite) {
        val datasetObj = getDatasetObj(dataset)
        truncateDataset(datasetObj, version)
      }

      val numPartitions = df.rdd.partitions.size
      logger.info(s"Inserting into ($dataset/$version) with $numPartitions partitions")

      // For each partition, start the ingestion
      df.rdd.mapPartitionsWithIndex { case (index, rowIter) =>
        // Everything within this function runs on each partition/executor, so need a local datastore & system
        FiloSetup.init(filoConfig)
        logger.info(s"Starting ingestion of DataFrame for dataset $dataset, partition $index...")
        ingestRddRows(FiloSetup.coordinatorActor, dataset, columnNames, version, rowIter,
                      writeTimeout, index)
        Iterator.empty
      }.count()

      syncToHive(sqlContext)
    }
  }
}