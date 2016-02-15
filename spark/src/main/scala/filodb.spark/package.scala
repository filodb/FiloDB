package filodb

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame, Row, Column => SparkColumn}
import org.apache.spark.sql.types.DataType
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.coordinator.{NodeCoordinatorActor, RowSource, DatasetCoordinatorActor}

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
                    splitsPerNode: Int = 1): DataFrame =
      sqlContext.baseRelationToDataFrame(FiloRelation(dataset, version, splitsPerNode)
                                                     (sqlContext))

    private def runCommands[B](cmds: Set[Future[Response]]): Unit = {
      val responseSet = Await.result(Future.sequence(cmds), 5 seconds)
      if (!responseSet.forall(_ == Success)) throw new RuntimeException(s"Some commands failed: $responseSet")
    }

    import filodb.spark.TypeConverters._

    private def dfToFiloColumns(df: DataFrame): Seq[DataColumn] = {
      df.schema.map { f =>
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

    // This doesn't create columns, because that's in checkAndAddColumns.
    // It does check for schema errors though first.  :)
    private def createNewDataset(datasetName: String,
                                 rowKeys: Seq[String],
                                 segmentKey: String,
                                 partitionKeys: Seq[String],
                                 chunkSize: Option[Int],
                                 dfColumns: Seq[Column]): Unit = {
      val options = Dataset.DefaultOptions
      val options2 = chunkSize.map { newSize => options.copy(chunkSize = newSize) }.getOrElse(options)
      val dataset = Dataset(datasetName, rowKeys, segmentKey, partitionKeys).copy(options = options2)

      // validate against schema.  Checks key names, computed columns, etc.
      RichProjection.make(dataset, dfColumns).recover {
        case err: RichProjection.BadSchema => throw BadSchemaError(err.toString)
      }

      logger.info(s"Creating dataset $dataset...")
      actorAsk(FiloSetup.coordinatorActor, CreateDataset(dataset, Nil)) {
        case DatasetCreated =>
          logger.info(s"Dataset $datasetName created successfully...")
        case DatasetError(errMsg) =>
          throw new RuntimeException(s"Error creating dataset: $errMsg")
      }
    }

    private def truncateDataset(dataset: Dataset, version: Int): Unit = {
      logger.info(s"Truncating dataset ${dataset.name}")
      actorAsk(FiloSetup.coordinatorActor,
               TruncateProjection(dataset.projections.head, version), 1.minute) {
        case ProjectionTruncated => logger.info(s"Truncation of ${dataset.name} finished")
        case UnknownDataset => throw NotFoundError(s"(${dataset.name}, ${version}})")
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
    def saveAsFiloDataset(df: DataFrame,
                          dataset: String,
                          rowKeys: Seq[String],
                          segmentKey: String,
                          partitionKeys: Seq[String],
                          version: Int = 0,
                          chunkSize: Option[Int] = None,
                          mode: SaveMode = SaveMode.Append,
                          writeTimeout: FiniteDuration = DefaultWriteTimeout): Unit = {
      val filoConfig = FiloSetup.configFromSpark(sqlContext.sparkContext)
      FiloSetup.init(filoConfig)

      val partKeys = if (partitionKeys.nonEmpty) partitionKeys else Seq(Dataset.DefaultPartitionColumn)
      val dfColumns = dfToFiloColumns(df)

      try {
        val datasetObj = getDatasetObj(dataset)
        if (mode == SaveMode.Overwrite) truncateDataset(datasetObj, version)
      } catch {
        case e: NotFoundError =>
          createNewDataset(dataset, rowKeys, segmentKey, partKeys, chunkSize, dfColumns)
      }
      checkAndAddColumns(dfColumns, dataset, version)

      val numPartitions = df.rdd.partitions.size
      logger.info(s"Saving ($dataset/$version) with row keys $rowKeys, segment key $segmentKey, " +
                  s"partition keys $partKeys, $numPartitions partitions")
      ingestDF(df, filoConfig, dataset, dfColumns.map(_.name), version, writeTimeout)
    }

    def ingestDF(df: DataFrame, filoConfig: Config, dataset: String,
                 dfColumns: Seq[String], version: Int,
                 writeTimeout: FiniteDuration): Unit = {
      // For each partition, start the ingestion
      df.rdd.mapPartitionsWithIndex { case (index, rowIter) =>
        // Everything within this function runs on each partition/executor, so need a local datastore & system
        FiloSetup.init(filoConfig)
        logger.info(s"Starting ingestion of DataFrame for dataset $dataset, partition $index...")
        ingestRddRows(FiloSetup.coordinatorActor, dataset, dfColumns, version, rowIter,
                      writeTimeout, index)
        Iterator.empty
      }.count()
    }
  }
}