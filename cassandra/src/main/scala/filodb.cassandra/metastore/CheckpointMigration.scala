package filodb.cassandra.metastore

import java.lang.{Integer => JInt, Long => JLong}
import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.memstore.FiloSchedulers
import filodb.core.{DatasetRef, GlobalConfig}
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import scala.Console.println
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


sealed class CheckpointTableConnector(val keyspace: String,
                                      val config: Config,
                                      val session: Session,
                                      writeConsistencyLevel: ConsistencyLevel,
                                      checkPointReadConsistencyLevel: ConsistencyLevel)
                                      (implicit val ec: ExecutionContext) extends FiloCassandraConnector {
  val tableString = s"${keyspace}.checkpoints"

  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       | databasename text,
       | datasetname text,
       | shardnum int,
       | groupnum int,
       | offset bigint,
       | PRIMARY KEY ((databasename, datasetname, shardnum), groupnum)
       |)""".stripMargin

  lazy val readCheckpointCql =
    session.prepare(
      s"""SELECT groupnum, offset FROM $tableString WHERE
         | databasename = ? AND
         | datasetname = ? AND
         | shardnum = ? """.stripMargin).setConsistencyLevel(checkPointReadConsistencyLevel)
  // we want consistent reads during recovery

  lazy val writeCheckpointCql = {
    val statement = session.prepare(
      s"""INSERT INTO $tableString (databasename, datasetname, shardnum, groupnum, offset)
         | VALUES (?, ?, ?, ?, ?)""".stripMargin
    )
    statement.setConsistencyLevel(writeConsistencyLevel)
    statement
  }

  import filodb.cassandra.Util._
  import filodb.core._

  def initialize(): Future[Response] = execCql(createCql)

  def clearAll(): Future[Response] = execCql(s"TRUNCATE $tableString")

  def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response] = {
    // TODO database name should not be an optional in internally since there is a default value. Punted for later.
    execStmt(writeCheckpointCql.bind(dataset.database.getOrElse(""),
      dataset.dataset, shardNum: JInt, groupNum: JInt, offset: JLong))
  }

  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int, Long]] = {
    session.executeAsync(readCheckpointCql.bind(dataset.database.getOrElse(""),
      dataset.dataset, shardNum: JInt))
      .toIterator // future of Iterator
      .map { it => it.map(r => r.getInt(0) -> r.getLong(1)).toMap }
  }
}


object CheckpointMigration extends StrictLogging {

  def main(rawArgs: Array[String]): Unit = {
    val usage =
    """
      Usage:  [--shardnum num] [--preagg num] [--raw filename]
    """

    if (rawArgs.isEmpty || rawArgs.length != 6) {
      println(usage)
      System.exit(1)
    }

    val argMap = Map.newBuilder[String, Any]

    rawArgs.sliding(2, 2).toList.collect {
      case Array("--shardnum", shardnum: String) => argMap.+=("shardnum" -> shardnum.toInt)
      case Array("--preagg", preagg: String) => argMap.+=("preagg" -> preagg)
      case Array("--raw", raw: String) => argMap.+=("raw" -> raw)
    }
    val args = argMap.result()
    println(args)


    val allConfig = GlobalConfig.configToDisableAkkaCluster.withFallback(GlobalConfig.systemConfig)
    val filoConfig = allConfig.getConfig("filodb")
    val cassandraConfig = filoConfig.getConfig("cassandra")
    val session = FiloSessionProvider.openSession(cassandraConfig)
    val ingestionConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("ingestion-consistency-level"))
    val checkpointReadConsistencyLevel = ConsistencyLevel.valueOf(
       cassandraConfig.getString("checkpoint-read-consistency-level"))

    val shardNum = args.get("shardnum").get.asInstanceOf[Int]
    logger.info(s"filoConfig =${filoConfig}")

    lazy val ioPool = Scheduler.io(name = FiloSchedulers.IOSchedName,
        reporter = UncaughtExceptionReporter(
          m => println(s"Uncaught Exception in FilodbCluster.ioPool =${m}")))
    val rawMetaStore = new CheckpointTableConnector(
      args.get("raw").get.asInstanceOf[String], cassandraConfig, session,
      ingestionConsistencyLevel, checkpointReadConsistencyLevel)(ioPool)
    val preaggMetaStore = new CheckpointTableConnector(args.get("preagg").get.asInstanceOf[String], cassandraConfig, session,
      ingestionConsistencyLevel, checkpointReadConsistencyLevel)(ioPool)

    val preaggDataRef = DatasetRef("prometheus_preagg")
    for (shard <- 0 until shardNum) {
      val read = preaggMetaStore.readCheckpoints(preaggDataRef, shard)
      read.onComplete({
        case Success(result) => {
          println(s"result=${result}")
          logger.info(s"result=${result}")
          result.foreach {
            case (group, offset) => {
              rawMetaStore.writeCheckpoint(preaggDataRef, shard, group, offset)
            }
          }
        }
        case Failure(exception) => exception.printStackTrace()
      })(ioPool)
    }
  }
}
