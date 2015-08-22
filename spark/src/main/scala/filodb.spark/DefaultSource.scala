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
   * Parameters:
   *   dataset
   *   version          defaults to 0
   *   write_timeout    set the timeout
   *   create_dataset   set to true to create a dataset, defaults to false
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt

    val createDataset = parameters.getOrElse("create_dataset", "false").toBoolean

    val config = ConfigFactory.parseMap(parameters.asJava).withFallback(DefaultConfig)

    sqlContext.saveAsFiloDataset(data, config, dataset, version,
                                 createDataset=createDataset)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(config, dataset, version)(sqlContext)
  }
}
