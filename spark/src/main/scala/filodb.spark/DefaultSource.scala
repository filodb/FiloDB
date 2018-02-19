package filodb.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._

import filodb.core.DatasetRef

/**
 * DefaultSource implements the Spark Dataframe read() and write() API for FiloDB.
 * This also enables Spark SQL / JDBC "CREATE TABLE" DDLs without any use of Scala.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider {
  val DefaultSplitsPerNode = "4"

  /**
   * Implements dataframe.read() functionality.
   * Parameters:
   *   dataset
   *   database         defaults to filodb.cassandra.keyspace
   *   splits_per_node  defaults to 4, the number of splits or read threads per node
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // dataset is a mandatory parameter.  Need to know the name.
    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified for FiloDB."))
    val database = parameters.get("database")
    val splitsPerNode = parameters.getOrElse("splits_per_node", DefaultSplitsPerNode).toInt
    FiloRelation(DatasetRef(dataset, database), splitsPerNode = splitsPerNode)(sqlContext)
  }

  /**
   * Implements dataframe.write()
   * Note: SaveMode.Overwrite means to create a new dataset
   * Parameters:
   *   dataset
   *   database         defaults to filodb.cassandra.keyspace
   *   row_keys         comma-separated list of row keys
   *   partition_columns comma-separated list of partition column name:type pairs
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
    val rowKeys = parameters.get("row_keys").map(_.split(',').toSeq).getOrElse(Nil)
    val partitionCols = parameters.get("partition_columns").get.split(',').toSeq
    val flushAfter = parameters.get("flush_after_write").map(_.toBoolean).getOrElse(true)
    val resetSchema = parameters.get("reset_schema").map(_.toBoolean).getOrElse(false)

    val options = IngestionOptions(flushAfterInsert = flushAfter,
                                   resetSchema = resetSchema)
    sqlContext.saveAsFilo(data, dataset, rowKeys, partitionCols, database, mode, options)

    // The below is inefficient as it reads back the schema that was written earlier - though it shouldn't
    // take very long
    FiloRelation(DatasetRef(dataset, database))(sqlContext)
  }
}
