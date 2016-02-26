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
   *   row_keys         comma-separated list of row keys
   *   segment_key
   *   partition_keys   comma-separated list of partition keys
   *   chunk_size       defaults to 5000
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val version = parameters.getOrElse("version", "0").toInt
    val rowKeys = parameters.getOrElse("row_keys", sys.error("'row_keys' must be specified.")).
                             split(',').toSeq
    val segKey  = parameters.getOrElse("segment_key", sys.error("'segment_key' must be specified."))
    val partitionKeys = parameters.get("partition_keys").map(_.split(',').toSeq).getOrElse(Nil)
    val chunkSize = parameters.get("chunk_size").map(_.toInt)

    sqlContext.saveAsFilo(data, dataset,
                          rowKeys, segKey, partitionKeys, version,
                          chunkSize, mode)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(dataset, version)(sqlContext)
  }
}
