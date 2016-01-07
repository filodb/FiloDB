package filodb.spark.client

import filodb.spark._
import filodb.spark.Filo
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.concurrent.Await
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Filo Interpreter to parse and execute the commands sent through CLI.
  */

object FiloInterpreter {
  //scalastyle:off
  var sc: SparkContext = null
  var sql: SQLContext = null
  private lazy val dfFailure = sql.read.json(sc.parallelize(Seq( """{"Filo-status":0}"""), 1))
  private lazy val dfSuccess = sql.read.json(sc.parallelize(Seq( """{"Filo-status":1}"""), 1))
  //scalastyle:on
  /** To initialize the columnStore, metaStore and the SqlContext and SparkContext
    * @param sContext the spark context to be used for initialization
    */
  def init(sContext: SparkContext): Unit = {
    sc = sContext
    sql = new SQLContext(sc)
    Filo.init(configFromSpark(sc))
    Filo.parse(Filo.columnStore.initialize)(x => x)
    Filo.parse(Filo.metaStore.initialize)(x => x)
    val description = Filo.metaStore.projectionTable.getAllSuperProjectionNames
    val result = for {
      infoAll <- description
    } yield {
      infoAll.foreach { name => val tableDS = sql.read.format("filodb.spark").option("dataset", name).load()
        tableDS.registerTempTable(name)
      }
    }
  }

  /** The method to be called in the end to clear the columnStore and metaStore */
  def stop(): Unit = {
    sc.stop()
  }

  /** Interprets load, create, select, show and describe statements and executes them
    * @param input Statement to be parsed and executed if valid
    */
  def interpret(input: String): DataFrame = {
    input.toLowerCase.trim match {
      case select: String if select.startsWith("select") =>
        if (SimpleParser.parseSelect(input)) {
          sql.sql(StringUtils.removeEnd(input, ";"))
        }
        else {
          dfFailure
        }
      case create: String if create.startsWith("create") =>
        val create = SimpleParser.parseCreate(input)
        FiloExecutor.handleCreate(create, sql, dfSuccess)
      case load: String if load.startsWith("load") =>
        val load = SimpleParser.parseLoad(input)
        val dataDF = sql.read.format(load.format).options(load.options).load(load.url)
        sql.saveAsFiloDataset(dataDF, load.tableName)
        val tableDS = sql.read.format("filodb.spark").option("dataset", load.tableName).load()
        tableDS.registerTempTable(load.tableName)
        dfSuccess
      case show: String if show.startsWith("show") =>
        FiloExecutor.handleShow(input, sql, sc, dfFailure)
      case describe: String if describe.startsWith("describe") =>
        val describe = SimpleParser.parseDecribe(input)
        FiloExecutor.handleDescribe(describe, sql, sc, dfFailure)
      case _ => throw new IllegalArgumentException("Cannot parse the given statement")
    }
  }

  /** Converts the dataframe into String with the max number of rows as numRows
    * @param dataframe the dataframe to be converted into string
    * @param numRows max number of rows to be displayed
    */
  def dfToString(dataframe: DataFrame, numRows: Int): String = {
    val sb = new StringBuilder
    val data = dataframe.take(numRows)
    val numCols = dataframe.schema.fieldNames.length
    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    val rows: Seq[Seq[String]] = dataframe.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        //scalastyle:off
        val str = if (cell == null) {
          //scalastyle:on
          "null"
        }
        else {
          cell.toString
        }
        if (str.length > 20) str.substring(0, 17) + "..." else str
      }: Seq[String]
    }
    // Compute the width of each column
    val colWidths = Array.fill(numCols)(0)
    for {row <- rows} {
      for {(cell, i) <- row.zipWithIndex} {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }
    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()
    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      StringUtils.leftPad(cell.toString, colWidths(i))
    }.addString(sb, "|", "|", "|\n")
    sb.append(sep)
    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        StringUtils.leftPad(cell.toString, colWidths(i))
      }.addString(sb, "|", "|", "|\n")
    }
    sb.append(sep)
    sb.toString()
  }

}
