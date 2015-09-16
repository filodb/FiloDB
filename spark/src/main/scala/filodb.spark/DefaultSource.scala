package filodb.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._

object DefaultSource {
  val DefaultConfigStr = """
    max-outstanding-futures = 16
  """
  val DefaultConfig = ConfigFactory.parseString(DefaultConfigStr)
}

/**
 * DefaultSource implements the Spark Dataframe read() and write() API for FiloDB.
 * This also enables Spark SQL / JDBC "CREATE TABLE" DDLs without any use of Scala.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider {
  import collection.JavaConverters._
  import DefaultSource._

  /**
   * Implements dataframe.read() functionality.
   * Parameters:
   *   dataset
   *   version          defaults to 0
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // dataset is a mandatory parameter.  Need to know the name.
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt

    val config = ConfigFactory.parseMap(parameters.asJava).withFallback(DefaultConfig)
    FiloRelation(config, dataset, version)(sqlContext)
  }

  /**
   * Implements dataframe.write()
   * Note: SaveMode.Overwrite means to create a new dataset
   * Parameters:
   *   dataset
   *   version          defaults to 0
   *   partition_column name of the partitioning column
   *   sort_column      name of the sort column within each partition
   *   write_timeout    set the timeout
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val sortColumn = parameters.getOrElse("sort_column", sys.error("'sort_column' must be specified"))
    val partitionColumn = parameters.getOrElse("partition_column",
                                               sys.error("'partition_column' must be specified"))

    val createDataset = mode == SaveMode.Overwrite

    val config = ConfigFactory.parseMap(parameters.asJava).withFallback(DefaultConfig)

    sqlContext.saveAsFiloDataset(data, config, dataset, version,
                                 sortColumn, partitionColumn,
                                 createDataset=createDataset)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(config, dataset, version)(sqlContext)
  }
}
