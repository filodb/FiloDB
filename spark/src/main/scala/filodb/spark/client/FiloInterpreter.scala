package filodb.spark.client

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.spark._
import filodb.core.store.Dataset
import filodb.spark.Filo
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import scala.concurrent.{Future, Await}
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success,Failure}

object FiloInterpreter {

  val conf = (new SparkConf(false)).setMaster("local[4]")
    .setAppName("test")
    .set("spark.filodb.cassandra.hosts", "localhost")
    .set("spark.filodb.cassandra.port", "9142")
    .set("spark.filodb.cassandra.keyspace", "unittest")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  def init() :Unit  = {
    Filo.init(configFromSpark(sc))
    Await.result(Filo.columnStore.initialize, 10 seconds)
    Await.result(Filo.metaStore.initialize, 10 seconds)
  }

  def stop():Unit ={
    Await.result(Filo.columnStore.clearAll, 10 seconds)
    Await.result(Filo.metaStore.clearAll, 10 seconds)
    sc.stop()
  }

  def getSparkContext : SparkContext = sc

  def getSqlContext: SQLContext = sql

  def interpret(input: String): Any = {
    input.toLowerCase.trim match {

      case s: String if s.startsWith("s") =>
        if (SimpleParser.parseSelect(input)) {
          val df = sql.sql(input)
          df.show(20)
          return df
        }

      case c: String if c.startsWith("c") =>
        val create = SimpleParser.parseCreate(input)
        val columns = create.columns map {
          case (colName, colType) =>
            Column(colName, create.tableName, 0, ColumnType.withName(colType))
        } toSeq
        val dataset = Dataset.apply(create.tableName, columns,
          create.partitionCols, create.primaryCols, create.sortCols, create.segmentCols)
         Filo.metaStore.addProjection(dataset.projectionInfoSeq.head).onComplete{
          case Success(value) => true
          case Failure(value) => false
        }

      case l: String if l.startsWith("l") =>
        val load = SimpleParser.parseLoad(input)
        val dataDF = sql.read.format(load.format).options(load.options).load(load.url)
        sql.saveAsFiloDataset(dataDF, load.tableName)
        true

      case _ => throw new IllegalArgumentException("Cannot parse the given statement")
    }
  }
}
