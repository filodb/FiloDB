package filodb.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._

/**
 * DefaultSource implements the Spark Dataframe read() and write() API for FiloDB.
 * This also enables Spark SQL / JDBC "CREATE TABLE" DDLs without any use of Scala.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider {
  import collection.JavaConverters._

  val DefaultSplitsPerNode = "4"

  /**
   * Implements dataframe.read() functionality.
   * Parameters:
   *   dataset
   *   version          defaults to 0
   *   splits_per_node  defaults to 4, the number of splits or read threads per node
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // dataset is a mandatory parameter.  Need to know the name.
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val splitsPerNode = parameters.getOrElse("splits_per_node", DefaultSplitsPerNode).toInt
    FiloRelation(dataset, version, splitsPerNode = splitsPerNode)(sqlContext)
  }

  /**
   * Implements dataframe.write()
   * Note: SaveMode.Overwrite means to create a new dataset
   * Parameters:
   *   dataset
   *   version          defaults to 0
   *   partition_column name of the partitioning column
   *   sort_column      name of the sort column within each partition
   *   default_partition_key if defined, use this as the partition key when partitioning column is null
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val sortColumn = parameters.getOrElse("sort_column", sys.error("'sort_column' must be specified"))
    val partitionColumn = parameters.get("partition_column")
    val defaultPartKey = parameters.get("default_partition_key")
    val segmentSize = parameters.get("segment_size")

    sqlContext.saveAsFiloDataset(data, dataset,
                                 sortColumn, partitionColumn, version,
                                 mode, defaultPartKey, segmentSize)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(dataset, version)(sqlContext)
  }
}
