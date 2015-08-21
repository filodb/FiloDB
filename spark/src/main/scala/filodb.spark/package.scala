package filodb

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.DataType
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.cassandra.CassandraDatastore
import filodb.core.datastore.Datastore
import filodb.core.ingest.{CoordinatorActor, RowSource}
import filodb.core.metadata.{Column, Dataset, Partition}
import filodb.core.messages._

package spark {
  case class DatasetNotFound(dataset: String) extends Exception
  // For each mismatch: the column name, DataFrame type, and existing column type
  case class ColumnTypeMismatch(mismatches: Set[(String, DataType, Column.ColumnType)]) extends Exception
}

/**
 * Provides base methods for reading from and writing to FiloDB tables/datasets.
 * Note that this is not the recommended DataFrame load/save API, please see DefaultSource.scala.
 * Here is how you could use these APIs
 *
 * {{{
 *   > import filodb.spark._
 *   > val config = com.typesafe.config.ConfigFactory.parseString("max-outstanding-futures = 16")
 *   > sqlContext.saveAsFiloDataset(myDF, config, "table1", createDataset=true)
 *
 *   > sqlContext.filoDataset(config, "table1")
 * }}}
 */
package object spark extends StrictLogging {
  val DefaultWriteTimeout = 999 minutes

  implicit class FiloContext(sqlContext: SQLContext) {
    implicit val context = scala.concurrent.ExecutionContext.Implicits.global

    /**
     * Creates a DataFrame from a FiloDB table.  Does no reading until a query is run, but it does
     * read the schema for the table.
     * @param filoConfig the Configuration for connecting to FiloDB
     * @param dataset the name of the FiloDB table/dataset to read from
     * @param version the version number to read from
     */
    def filoDataset(filoConfig: Config,
                    dataset: String,
                    version: Int = 0,
                    minPartitions: Int = FiloRelation.DefaultMinPartitions): DataFrame =
      sqlContext.baseRelationToDataFrame(FiloRelation(filoConfig, dataset, version, minPartitions)(sqlContext))

    private def runCommands[B](cmds: Set[Future[Response]]): Unit = {
      val responseSet = Await.result(Future.sequence(cmds), 5 seconds)
      if (!responseSet.forall(_ == Success)) throw new RuntimeException(s"Some commands failed: $responseSet")
    }

    import filodb.spark.TypeConverters._
    import filodb.spark.FiloRelation._

    private def checkAndAddColumns(datastore: Datastore,
                                   df: DataFrame,
                                   dataset: String,
                                   version: Int): Unit = {
      // Pull out existing dataset schema
      val schema = parseResponse(datastore.getSchema(dataset, version)) {
        case Datastore.TheSchema(schemaObj) =>
          logger.info(s"Read schema for dataset $dataset = $schemaObj")
          schemaObj
      }

      // Translate DF schema to columns, create new ones if needed
      val namesTypes = df.schema.map { f => f.name -> f.dataType }.toMap
      val matchingCols = namesTypes.keySet.intersect(schema.keySet)
      val missingCols = namesTypes.keySet -- schema.keySet
      logger.info(s"Matching columns - $matchingCols\nMissing columns - $missingCols")

      // Type-check matching columns
      val matchingTypeErrs = matchingCols.collect {
        case colName: String if sqlTypeToColType(namesTypes(colName)) != schema(colName).columnType =>
          (colName, namesTypes(colName), schema(colName).columnType)
      }
      if (matchingTypeErrs.nonEmpty) throw ColumnTypeMismatch(matchingTypeErrs)

      if (missingCols.nonEmpty) {
        val addMissingCols = missingCols.map { colName =>
          val newCol = Column(colName, dataset, version, sqlTypeToColType(namesTypes(colName)))
          datastore.newColumn(newCol)
        }
        runCommands(addMissingCols)
      }
    }

    private def checkAndAddPartitions(datastore: Datastore,
                                      df: DataFrame,
                                      dataset: String,
                                      createDataset: Boolean = false): Unit = {
      val numPartitions = df.rdd.partitions.size
      val dfPartNames = (0 until numPartitions).map(_.toString)

      val datasetObj = parseResponse(datastore.getDataset(dataset)) {
        case Datastore.TheDataset(datasetObj) => datasetObj
        case NotFound =>
          if (createDataset) {
            logger.info(s"Dataset $dataset not found, creating...")
            runCommands(Set(datastore.newDataset(dataset)))
            Dataset(dataset, Set.empty)
          } else {
            throw DatasetNotFound(dataset)
          }
      }
      val missingPartitions = dfPartNames.toSet -- datasetObj.partitions
      if (missingPartitions.nonEmpty) {
        logger.info(s"Adding missing partitions - $missingPartitions")
        val addMissingParts = missingPartitions.map(Partition(dataset, _)).map(datastore.newPartition)
        runCommands(addMissingParts)
      }
    }

    /**
     * Saves a DataFrame in a FiloDB Table
     * - Creates columns in FiloDB from DF schema if needed
     * - Creates partitions if needed based on partition ordinal number
     * - Only overwrite supported for now, not appends
     *
     * @param df the DataFrame to write to FiloDB
     * @param filoConfig the Configuration for connecting to FiloDB
     * @param dataset the name of the FiloDB table/dataset to read from
     * @param version the version number to read from
     * @param createDataset if true, then creates a Dataset if one doesn't exist.  Defaults to false to
     *                      prevent accidental table creation.
     * @param writeTimeout Maximum time to wait for write of each partition to complete
     */
    def saveAsFiloDataset(df: DataFrame,
                          filoConfig: Config,
                          dataset: String,
                          version: Int = 0,
                          createDataset: Boolean = false,
                          writeTimeout: FiniteDuration = DefaultWriteTimeout): Unit = {

      val datastore = new CassandraDatastore(filoConfig)

      checkAndAddPartitions(datastore, df, dataset, createDataset)
      checkAndAddColumns(datastore, df, dataset, version)
      val dfColumns = df.schema.map(_.name)

      // For each partition, start the ingestion
      df.rdd.mapPartitionsWithIndex { case (index, rowIter) =>
        // Everything within this function runs on each partition/executor, so need a local datastore & system
        val _datastore = new CassandraDatastore(filoConfig)
        val _system = ActorSystem(s"partition_$index")
        val coordinator = _system.actorOf(CoordinatorActor.props(_datastore))
        logger.info(s"Starting ingestion of DataFrame for dataset $dataset, partition $index...")
        val ingestActor = _system.actorOf(RddRowSourceActor.props(rowIter, dfColumns,
                                          dataset, index.toString, version, coordinator))
        implicit val timeout = Timeout(writeTimeout)
        val res = Await.result(ingestActor ? RowSource.Start, writeTimeout)
        Iterator.empty
      }.count()
    }
  }
}