package org.apache.spark.sql.hive.filodb

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

import filodb.core.store.MetaStore
import filodb.spark.FiloRelation

object MetaStoreSync extends StrictLogging {
  import filodb.coordinator.client.Client.parse

  /**
   * Tries to get a HiveContext either from a running ThriftServer or from the sqlcontext that's
   * passed in.
   */
  def getHiveContext(sqlContext: SQLContext): Option[HiveContext] = {
    sqlContext match {
      case hc: HiveContext => Some(hc)
      case other: Any      => None
    }
  }

  /**
   * Syncs Filo tables in the metastore to the Hive MetaStore, adding an entry in the MetaStore for
   * Filo tables which are missing.  By default, only works in the keyspace pointed to in the Filo
   * configuration.
   * @param databaseName the Hive MetaStore database name to sync with
   * @param the FiloDB MetaStore
   * @param hiveContext the HiveContext containing the catalog to sync to
   */
  def syncFiloTables(databaseName: String, metastore: MetaStore, hiveContext: HiveContext): Int = {
    val catalog = hiveContext.catalog
    val hiveTables = catalog.getTables(Some(databaseName)).map(_._1)
    val filoTables = parse(metastore.getAllDatasets(databaseName)) { ds => ds }
    val missingTables = filoTables.toSet -- hiveTables.toSet
    logger.info(s"Syncing FiloDB tables to Hive MetaStore.  Missing tables = $missingTables")

    missingTables.toSeq.foreach { missingTable =>
      logger.info(s"Creating external FiloDB table $missingTable in Hive database $databaseName")
      val ident = TableIdentifier(missingTable, Some(databaseName))
      catalog.createDataSourceTable(ident, None, Array[String](),
                                    provider = "filodb.spark",
                                    options = Map("dataset" -> missingTable),
                                    isExternal = true)
    }
    missingTables.size
  }
}