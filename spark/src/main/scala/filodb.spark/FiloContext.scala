package filodb.spark

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.types.StructType

import filodb.coordinator.NodeClusterActor
import filodb.coordinator.client.Client
import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store.StoreConfig

/**
 * Class implementing insert and save Scala APIs.
 * Don't directly instantiate this, instead use the implicit conversion function.
 */
class FiloContext(val sqlContext: SQLContext) extends AnyVal {
  import Client.parse
  import FiloRelation._

  /**
   * Creates a DataFrame from a FiloDB table.  Does no reading until a query is run, but it does
   * read the schema for the table.
   * @param dataset the name of the FiloDB dataset to read from
   * @param database the database / Cassandra keyspace to read the dataset from
   * @param splitsPerNode the parallelism or number of splits per node
   */
  def filoDataset(dataset: String,
                  database: Option[String] = None,
                  splitsPerNode: Int = 4): DataFrame =
    sqlContext.baseRelationToDataFrame(FiloRelation(DatasetRef(dataset, database), splitsPerNode)
                                                   (sqlContext))

  // Convenience method for Java programmers
  def filoDataset(dataset: String): DataFrame = filoDataset(dataset, None)

  /**
   * Creates (or recreates) a FiloDB dataset, writing to the MetaStore.
   * The exact behavior depends on the mode:
   *   Append  - creates the dataset if it doesn't exist
   *   Overwrite - creates the dataset, deleting the old definition first if needed
   *   ErrorIfExists - throws an error if the dataset already exists
   *
   * For the other parameter definitions, please see saveAsFiloDataset().
   */
  private[spark] def createOrUpdateDataset(schema: StructType,
                                           dataset: DatasetRef,
                                           rowKeys: Seq[String],
                                           partitionColumns: Seq[String],
                                           resetSchema: Boolean = false,
                                           mode: SaveMode = SaveMode.Append): Unit = {
    FiloDriver.init(sqlContext.sparkContext)
    val dfColumns = dfToFiloColumns(schema)

    val datasetObj = try {
      Some(getDatasetObj(dataset))
    } catch {
      case e: NotFoundError => None
    }
    (datasetObj, mode) match {
      case (None, SaveMode.Append) | (None, SaveMode.Overwrite) | (None, SaveMode.ErrorIfExists) =>
        val ds = makeAndVerifyDataset(dataset, rowKeys, partitionColumns, dfColumns)
        createNewDataset(ds, dataset.database)
      case (Some(dsObj), SaveMode.ErrorIfExists) =>
        throw new RuntimeException(s"Dataset $dataset already exists!")
      case (Some(dsObj), SaveMode.Overwrite) if resetSchema =>
        val ds = makeAndVerifyDataset(dataset, rowKeys, partitionColumns, dfColumns)
        parse(FiloDriver.metaStore.deleteDataset(dataset)) { x => x }
        createNewDataset(ds, dataset.database)
      case (_, _) =>
        sparkLogger.info(s"Dataset $dataset definition not changed")
    }
  }

  /**
   * Saves a DataFrame in a FiloDB Table.
   * Depending on the SaveMode, a Dataset definition may be created from the DataFrame schema.
   *
   * @param df the DataFrame to write to FiloDB
   * @param dataset the name of the FiloDB table/dataset to read from
   * @param rowKeys the name of the column(s) used as the row primary key within each partition.
   *                Cannot be computed.  May be used for range queries within partitions.
   * @param partitionColumns column name:type strings used to define partition columns.  For valid type strings
   *          please see Column.ColumnType.nameType values (or the README, or the filo-cli help)
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
   * @param database the database/keyspace to write to, optional.  Default behavior depends on ColumnStore.
   * @param mode the Spark SaveMode - ErrorIfExists, Append, Overwrite, Ignore
   * @param options various IngestionOptions, such as timeouts, version to write to, etc.
   */
  def saveAsFilo(df: DataFrame,
                 dataset: String,
                 rowKeys: Seq[String],
                 partitionColumns: Seq[String],
                 database: Option[String] = None,
                 mode: SaveMode = SaveMode.Append,
                 options: IngestionOptions = IngestionOptions()): Unit = {
    val IngestionOptions(writeTimeout, flushAfterInsert, resetSchema) = options
    val ref = DatasetRef(dataset, database)
    createOrUpdateDataset(df.schema, ref, rowKeys, partitionColumns, resetSchema, mode)
    insertIntoFilo(df, dataset, mode == SaveMode.Overwrite,
                   database, writeTimeout, flushAfterInsert)
  }

  import NodeClusterActor._

  /**
   * Implements INSERT INTO into a Filo Dataset.  The dataset must already have been created.
   * Will check columns from the DataFrame against the dataset definition. Column type
   * mismatches and missing columns will result in an error.
   * @param overwrite if true, first truncate the dataset before writing
   */
  // scalastyle:off
  def insertIntoFilo(df: DataFrame,
                     datasetName: String,
                     overwrite: Boolean = false,
                     database: Option[String] = None,
                     writeTimeout: FiniteDuration = DefaultWriteTimeout,
                     flushAfterInsert: Boolean = true): Unit = {
    FiloDriver.init(sqlContext.sparkContext)
    val dfColumns = dfToFiloColumns(df)
    val ref = DatasetRef(datasetName, database)
    val dataset = getDatasetObj(ref)
    checkColumns(dfColumns, dataset)

    if (overwrite) {
      FiloDriver.client.truncateDataset(ref)
    }

    val numPartitions = df.rdd.partitions.size
    sparkLogger.info(s"Inserting into ($ref) with $numPartitions partitions")
    sparkLogger.debug(s"   Dataframe schema = $dfColumns")

    val storeConf = ConfigFactory.parseString(s"""
                         |  store {
                         |    flush-interval = 30m
                         |    shard-memory-mb = 500
                         |  }""".stripMargin)

    // TODO: actually figure out right number of nodes.
    FiloDriver.client.setupDataset(ref,
                                   DatasetResourceSpec(numPartitions, Math.min(numPartitions, 10)),
                                   noOpSource, StoreConfig(storeConf)) match {
      case None =>
        sparkLogger.info(s"Ingestion set up on all coordinators for $ref")
        sparkLogger.info(s"Waiting to ensure coordinators ready. TODO: replace with shard status")
        Thread sleep 5000
      case Some(err) =>
        throw new RuntimeException(s"Error setting up ingestion: $err")
    }

    // Things to serialize
    val serializedDataset = dataset.asCompactString
    val dfColumnNames = dfColumns.map(_.split(':')(0))

    // For each partition, start the ingestion
    df.rdd.mapPartitionsWithIndex { case (index, rowIter) =>
      // Everything within this function runs on each partition/executor, so need a local datastore & system
      val _dataset = Dataset.fromCompactString(serializedDataset)
      sparkLogger.info(s"Starting ingestion of DataFrame for dataset ${_dataset.ref}, partition $index...")
      sparkLogger.warn(s"INGESTION IS NOT IMPLEMENTED RIGHT NOW.")
      ingestRddRows(FiloExecutor.clusterActor, _dataset, rowIter, writeTimeout, index)
      Iterator.empty
    }.count()

    // This is the only time that flush is explicitly called
    if (flushAfterInsert) flushAndLog(ref)

    syncToHive(sqlContext)
  }
  // scalastyle:on
}
