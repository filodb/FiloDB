package org.apache.spark.sql.hive.filodb

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import scala.util.Try

import filodb.core.store.MetaStore
import filodb.spark.FiloRelation


object MetaStoreSync extends StrictLogging {
  import filodb.coordinator.client.Client.parse

  def sparkHost: String =
    Try(SparkEnv.get.rpcEnv.address.host).getOrElse(java.net.InetAddress.getLocalHost.getHostAddress)

  /**
   * Tries to get a SparkSession either from a running ThriftServer or from the sqlcontext that's
   * passed in.
   */
  def getSparkSession(sqlContext: SQLContext): Option[SparkSession] = {
    Option(sqlContext.sparkSession)
  }

  /**
   * Syncs Filo tables in the metastore to the Hive MetaStore, adding an entry in the MetaStore for
   * Filo tables which are missing.  By default, only works in the keyspace pointed to in the Filo
   * configuration.
   * @param databaseName the Hive MetaStore database name to sync with
   * @param metastore FiloDB MetaStore
   * @param sparkSession the SparkSession containing the catalog to sync to
   */
  def syncFiloTables(databaseName: String, metastore: MetaStore, sparkSession: SparkSession): Int = {
    val catalog = sparkSession.catalog
    val hiveTables = catalog.listTables(databaseName).collect().map(_.name)
    val filoTables = parse(metastore.getAllDatasets(Some(databaseName))) { ds => ds.map(_.dataset) }
    val missingTables = filoTables.toSet -- hiveTables.toSet
    logger.info(s"Syncing FiloDB tables to Hive MetaStore.  Missing tables = $missingTables")

    missingTables.toSeq.foreach { missingTable =>
      logger.info(s"Creating external FiloDB table $missingTable in Hive database $databaseName")
      val ident = TableIdentifier(missingTable, Some(databaseName))
      CreateDataSourceTableUtils.createDataSourceTable(
        sparkSession, ident, None, Array[String](), None, "filodb.spark", Map("dataset" -> missingTable), true)
    }
    missingTables.size
  }
}
