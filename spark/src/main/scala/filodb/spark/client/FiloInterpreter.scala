package filodb.spark.client

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.spark._
import filodb.core.store.{ProjectionInfo, Dataset}
import filodb.spark.Filo
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.concurrent.{Future, Await}
import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

/**
  * Filo Interpreter to parse and execute the commands sent through CLI.
  *
  */

object FiloInterpreter {
  //scalastyle:off
  var sc: SparkContext = null
  var sql: SQLContext = null

  //scalastyle:on
  /** To initialize the columnStore, metaStore and the SqlContext and SparkContext
    * @param sContext the spark context to be used for initialization
    */
  def init(sContext: SparkContext): Unit = {
    sc = sContext
    sql = new SQLContext(sc)
    Filo.init(configFromSpark(sc))
    Await.result(Filo.columnStore.initialize, 10 seconds)
    Await.result(Filo.metaStore.initialize, 10 seconds)
  }

  /** The method to be called in the end to clear the columnStore and metaStore */
  def stop(): Unit = {
    Await.result(Filo.columnStore.clearAll, 10 seconds)
    Await.result(Filo.metaStore.clearAll, 10 seconds)
    sc.stop()
  }

  /** Interprets load, create, select, show and describe statements and executes them
    * @param input Statement to be parsed and executed if valid
    */
  def interpret(input: String): Future[DataFrame] = {
    input.toLowerCase.trim match {

      case select: String if select.startsWith("select") =>
        if (SimpleParser.parseSelect(input)) {
          Future {
            sql.sql(input)
          }
        }
        else {
          Future {
            dfFailure
          }
        }

      case create: String if create.startsWith("create") =>
        val create = SimpleParser.parseCreate(input)
        Future {
          handleCreate(create)
        }

      case load: String if load.startsWith("load") =>
        val load = SimpleParser.parseLoad(input)
        val dataDF = sql.read.format(load.format).options(load.options).load(load.url)
        sql.saveAsFiloDataset(dataDF, load.tableName)
        Future {
          dfSuccess
        }

      case show: String if show.startsWith("show") =>
        handleShow(input)

      case describe: String if describe.startsWith("describe") =>
        val describe = SimpleParser.parseDecribe(input)
        handleDescribe(describe)

      case _ => throw new IllegalArgumentException("Cannot parse the given statement")
    }
  }

  private lazy val dfFailure = sql.read.json(sc.parallelize(Seq( """{"Filo-status":0}"""), 1))
  private lazy val dfSuccess = sql.read.json(sc.parallelize(Seq( """{"Filo-status":1}"""), 1))

  /** handles the show tables action
    * @param input  the statement to be parsed for the show tables command
    */
  private def handleShow(input: String): Future[DataFrame] = {
    if (SimpleParser.parseShow(input)) {
      val description = Filo.metaStore.projectionTable.getAllSuperProjectionNames
      Filo.columnStore.summaryTable
      for {
        infoAll <- description
      } yield {
        val seq: Seq[String] = infoAll.map(name =>
          s"""{"dataset" : "$name"}""")
        sql.read.json(sc.parallelize(seq))
      }
    }
    else {
      Future {
        dfFailure
      }
    }
  }

  /** handles the create table action
    * @param create  the case class for describe consisting the parameters for create command
    */
  private def handleCreate(create: Create): DataFrame = {
    val columns = create.columns map {
      case (colName, colType) =>
        Column(colName, create.tableName, 0, ColumnType.withName(colType))
    } toSeq
    val dataset = Dataset.apply(create.tableName, columns,
      create.partitionCols, create.primaryCols, create.sortCols, create.segmentCols)
    Filo.metaStore.addProjection(dataset.projectionInfoSeq.head)
    val tableDS = sql.read.format("filodb.spark").option("dataset", create.tableName).load()
    tableDS.registerTempTable(create.tableName)
    dfSuccess
  }

  /** handles the describe tables or projections action
    * @param describe the case class for describe consisting the parameters for describe command
    */
  private def handleDescribe(describe: Describe): Future[DataFrame] = {
    if (describe.isTable) {
      val description = Filo.metaStore.projectionTable.getSuperProjection(describe.tableName)
      for {
        info <- description
      } yield {
        sql.read.json(sc.parallelize(Seq(
          s"""{"projection": "${info.id}",
                "dataset" : "${info.dataset}" ,
                "schema" :"${info.schema.map(col => col.name + " " + col.columnType).mkString(",")}" ,
                 "partitionCols": "${info.partitionColumns.mkString(",")}" ,
                  "primaryKey" : "${info.keyColumns.mkString(",")}" ,
                  "segmentColumns": "${info.segmentColumns.mkString(",")}" }"""), 1))
      }
    }
    else {
      describe.projectionName match {
        case Some(id) =>
          val description = Filo.metaStore.getProjection(describe.tableName, id)
          for {
            info <- description
          } yield {
            sql.read.json(sc.parallelize(Seq(
              s"""{"projection": "${info.id}",
                "dataset" : "${info.dataset}" ,
                "schema" :"${info.schema.map(col => col.name + " " + col.columnType).mkString(",")}" ,
                 "partitionCols": "${info.partitionColumns.mkString(",")}" ,
                  "primaryKey" : "${info.keyColumns.mkString(",")}" ,
                  "segmentColumns": "${info.segmentColumns.mkString(",")}" }"""), 1))
          }
        case _ => Future {
          dfFailure
        }
      }
    }

  }

  /** Converts the dataframe into String with the max number of rows as
   * @param numRows max number of rows to be displayed
   * @param dataframe the dataframe to be converted into string
   */
  def showString(numRows: Int, dataframe: DataFrame): String = {
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
