package filodb.spark

import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame, Row}
import org.apache.spark.sql.types.StructType
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}
import filodb.coordinator.client.ClientException
import filodb.coordinator.IngestionCommands

/**
 * Class implementing insert and save Scala APIs.
 * Don't directly instantiate this, instead use the implicit conversion function.
 */
class FiloContext(val sqlContext: SQLContext) extends AnyVal {
  import IngestionCommands.{Flush, Flushed}
  import FiloRelation._

  /**
   * Creates a DataFrame from a FiloDB table.  Does no reading until a query is run, but it does
   * read the schema for the table.
   * @param dataset the name of the FiloDB dataset to read from
   * @param database the database / Cassandra keyspace to read the dataset from
   * @param version the version number to read from
   * @param splitsPerNode the parallelism or number of splits per node
   */
  def filoDataset(dataset: String,
                  database: Option[String] = None,
                  version: Int = 0,
                  splitsPerNode: Int = 4): DataFrame =
    sqlContext.baseRelationToDataFrame(FiloRelation(DatasetRef(dataset, database), version, splitsPerNode)
                                                   (sqlContext))

  // Convenience method for Java programmers
  def filoDataset(dataset: String): DataFrame = filoDataset(dataset, None)

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
                                           dataset: DatasetRef,
                                           rowKeys: Seq[String],
                                           segmentKey: String,
                                           partitionKeys: Seq[String],
                                           chunkSize: Option[Int] = None,
                                           resetSchema: Boolean = false,
                                           mode: SaveMode = SaveMode.Append): Unit = {
    FiloDriver.init(sqlContext.sparkContext)
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
      case (Some(dsObj), SaveMode.Overwrite) if resetSchema =>
        val ds = makeAndVerifyDataset(dataset, rowKeys, segmentKey, partKeys, chunkSize, dfColumns)
        deleteDataset(dataset)
        createNewDataset(ds)
      case (_, _) =>
        sparkLogger.info(s"Dataset $dataset definition not changed")
    }
  }

  /**
   * Saves a DataFrame in a FiloDB Table
   * - Creates columns in FiloDB from DF schema if needed
   *
   * @param df the DataFrame to write to FiloDB
   * @param dataset the name of the FiloDB table/dataset to read from
   * @param rowKeys the name of the column(s) used as the row primary key within each partition.
   *                May be computed functions. Only used if mode is Overwrite and
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
   * @param database the database/keyspace to write to, optional.  Default behavior depends on ColumnStore.
   * @param mode the Spark SaveMode - ErrorIfExists, Append, Overwrite, Ignore
   * @param options various IngestionOptions, such as timeouts, version to write to, etc.
   */
  def saveAsFilo(df: DataFrame,
                 dataset: String,
                 rowKeys: Seq[String],
                 segmentKey: String,
                 partitionKeys: Seq[String],
                 database: Option[String] = None,
                 mode: SaveMode = SaveMode.Append,
                 options: IngestionOptions = IngestionOptions()): Unit = {
    val IngestionOptions(version, chunkSize, writeTimeout,
                         flushAfterInsert, resetSchema) = options
    val ref = DatasetRef(dataset, database)
    createOrUpdateDataset(df.schema, ref, rowKeys, segmentKey, partitionKeys, chunkSize, resetSchema, mode)
    insertIntoFilo(df, dataset, version, mode == SaveMode.Overwrite,
                   database, writeTimeout, flushAfterInsert)
  }

  /**
   * Implements INSERT INTO into a Filo Dataset.  The dataset must already have been created.
   * Will check and add any extra columns from the DataFrame into the dataset, but column type
   * mismatches will result in an error.
   * @param overwrite if true, first truncate the dataset before writing
   */
  def insertIntoFilo(df: DataFrame,
                     datasetName: String,
                     version: Int = 0,
                     overwrite: Boolean = false,
                     database: Option[String] = None,
                     writeTimeout: FiniteDuration = DefaultWriteTimeout,
                     flushAfterInsert: Boolean = true): Unit = {
    val filoConfig = FiloDriver.initAndGetConfig(sqlContext.sparkContext)
    val dfColumns = dfToFiloColumns(df)
    val columnNames = dfColumns.map(_.name)
    val dataset = DatasetRef(datasetName, database)
    checkAndAddColumns(dfColumns, dataset, version)

    if (overwrite) {
      FiloDriver.client.truncateDataset(dataset, version)
    }

    val numPartitions = df.rdd.partitions.size
    sparkLogger.info(s"Inserting into ($dataset/$version) with $numPartitions partitions")
    sparkLogger.debug(s"   Dataframe schema = $dfColumns")

    // For each partition, start the ingestion
    df.rdd.mapPartitionsWithIndex { case (index, rowIter) =>
      // Everything within this function runs on each partition/executor, so need a local datastore & system
      FiloExecutor.init(filoConfig)
      sparkLogger.info(s"Starting ingestion of DataFrame for dataset $dataset, partition $index...")
      ingestRddRows(FiloExecutor.coordinatorActor, dataset, columnNames, version, rowIter,
                    writeTimeout, index)
      Iterator.empty
    }.count()

    // This is the only time that flush is explicitly called
    if (flushAfterInsert) {
      try {
        val nodesFlushed = FiloDriver.client.flushCompletely(dataset, version)
        sparkLogger.info(s"Flush completed on $nodesFlushed nodes for dataset $dataset")
      } catch {
        case ClientException(msg) =>
          sparkLogger.warn(s"Could not flush due to client exception $msg on dataset $dataset...")
        case e: Exception =>
          sparkLogger.warn(s"Exception from flushing nodes for $dataset/$version", e)
      }
    }

    syncToHive(sqlContext)
  }
}
