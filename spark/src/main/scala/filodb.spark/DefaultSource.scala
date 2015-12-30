package filodb.spark

import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * DefaultSource implements the Spark Dataframe read() and write() API for FiloDB.
 * This also enables Spark SQL / JDBC "CREATE TABLE" DDLs without any use of Scala.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider {

  val DefaultSplitsPerNode = "4"
  val DefaultSplitSize = "100000"

  /**
   * Implements dataframe.read() functionality.
   * Parameters:
   * dataset
   * version          defaults to 0
   * splits_per_node  defaults to 4, the number of splits or read threads per node
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // dataset is a mandatory parameter.  Need to know the name.
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val splitsPerNode = parameters.getOrElse("splits_per_node", DefaultSplitsPerNode).toInt
    val splitSize = parameters.getOrElse("split-size", DefaultSplitSize).toInt
    val filoConfig = configFromSpark(sqlContext.sparkContext)
    FiloRelation(dataset, filoConfig, version, splitsPerNode, splitSize)(sqlContext)
  }

  /**
   * Implements dataframe.write()
   * Note: SaveMode.Overwrite means to create a new dataset
   * Parameters:
   * dataset
   * version          defaults to 0
   * partition_column name of the partitioning column
   * sort_column      name of the sort column within each partition
   * default_partition_key if defined, use this as the partition key when partitioning column is null
   */
  def createRelation(sqlContext: SQLContext,
                     mode: SaveMode,
                     parameters: Map[String, String],
                     data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val flushSize = parameters.getOrElse("flush-size", "1000").toInt
    val filoConfig = configFromSpark(sqlContext.sparkContext)
    sqlContext.saveAsFiloDataset(data, dataset, flushSize)
    FiloRelation(dataset, filoConfig, version)(sqlContext)
  }
}
