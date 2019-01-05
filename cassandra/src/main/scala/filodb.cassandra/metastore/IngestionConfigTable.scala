package filodb.cassandra.metastore

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.IngestionKeys
import filodb.core.store.{IngestionConfig, StoreConfig}

/**
 * Represents the "ingestionconfig" Cassandra table tracking streaming ingestion sources/definitions
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 * @param sessionProvider if provided, a session provider provides a session for the configuration
 */
sealed class IngestionConfigTable(val config: Config, val sessionProvider: FiloSessionProvider)
                                 (implicit val ec: ExecutionContext) extends FiloCassandraConnector {
  val keyspace = config.getString("admin-keyspace")
  val tableString = s"${keyspace}.ingestionconfig"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |database text,
                    |dataset text,
                    |resources text,
                    |factoryclass text,
                    |sourceconfig text,
                    |PRIMARY KEY ((database, dataset))
                    |)""".stripMargin

  import filodb.cassandra.Util._
  import filodb.core._

  def fromRow(row: Row): IngestionConfig = {
    val sourceConf = ConfigFactory.parseString(row.getString(IngestionKeys.SourceConfig))
    IngestionConfig(DatasetRef(row.getString("dataset"), Option(row.getString("database")).filter(_.length > 0)),
                    ConfigFactory.parseString(row.getString(IngestionKeys.Resources)),
                    row.getString("factoryclass"),
                    sourceConf,
                    StoreConfig(sourceConf.getConfig("store")))
  }

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  lazy val insertCql = session.prepare(
    s"""INSERT INTO $tableString (dataset, database, resources, factoryclass, sourceconfig
       |) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS""".stripMargin
  )

  def insertIngestionConfig(state: IngestionConfig): Future[Response] =
    execStmt(insertCql.bind(state.ref.dataset, state.ref.database.getOrElse(""),
                            state.resources.root.render(ConfigRenderOptions.concise),
                            state.streamFactoryClass,
                            state.streamStoreConfig.root.render(ConfigRenderOptions.concise)), AlreadyExists)

  // SELECT * with consistency ONE to let it succeed more often.  This is a temporary workaround to rearranging
  // the schema so we don't need to do a full table scan.  It is justified because the ingestion config table
  // almost never changes, this read is done only on new cluster startup, and the table is only written to with
  // the setup command (which always runs well after cluster startup).
  lazy val readAllCql = session.prepare(s"SELECT * FROM $tableString")
                          .setConsistencyLevel(ConsistencyLevel.ONE)

  def readAllConfigs(): Future[Seq[IngestionConfig]] =
    session.executeAsync(readAllCql.bind())
           .toIterator.map(_.map(fromRow).toSeq)

  def deleteIngestionConfig(dataset: DatasetRef): Future[Response] =
    execCql(s"DELETE FROM $tableString WHERE dataset = '${dataset.dataset}' AND " +
            s"database = '${dataset.database.getOrElse("")}' IF EXISTS", NotFound)
}