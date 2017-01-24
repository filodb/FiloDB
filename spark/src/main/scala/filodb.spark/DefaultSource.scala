package filodb.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._

import filodb.core.DatasetRef

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
   *   database         defaults to filodb.cassandra.keyspace
   *   version          defaults to 0
   *   splits_per_node  defaults to 4, the number of splits or read threads per node
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // dataset is a mandatory parameter.  Need to know the name.
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val database = parameters.get("database")
    val version = parameters.getOrElse("version", "0").toInt
    val splitsPerNode = parameters.getOrElse("splits_per_node", DefaultSplitsPerNode).toInt
    FiloRelation(DatasetRef(dataset, database), version, splitsPerNode = splitsPerNode)(sqlContext)
  }

  /**
   * Implements dataframe.write()
   * Note: SaveMode.Overwrite means to create a new dataset
   * Parameters:
   *   dataset
   *   database         defaults to filodb.cassandra.keyspace
   *   version          defaults to 0
   *   row_keys         comma-separated list of row keys
   *   partition_keys   comma-separated list of partition keys
   *   chunk_size       defaults to 5000
   *   flush_after_write  defaults to true
   *   reset_schema     defaults to false
   */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val database = parameters.get("database")
    val version = parameters.getOrElse("version", "0").toInt
    val rowKeys = parameters.get("row_keys").map(_.split(',').toSeq).getOrElse(Nil)
    val partitionKeys = parameters.get("partition_keys").map(_.split(',').toSeq).getOrElse(Nil)
    val chunkSize = parameters.get("chunk_size").map(_.toInt)
    val flushAfter = parameters.get("flush_after_write").map(_.toBoolean).getOrElse(true)
    val resetSchema = parameters.get("reset_schema").map(_.toBoolean).getOrElse(false)

    val options = IngestionOptions(version, chunkSize, flushAfterInsert = flushAfter,
                                   resetSchema = resetSchema)
    sqlContext.saveAsFilo(data, dataset, rowKeys, partitionKeys, database, mode, options)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(DatasetRef(dataset, database), version)(sqlContext)
  }
}
