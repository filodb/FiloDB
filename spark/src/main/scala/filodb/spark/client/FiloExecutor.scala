package filodb.spark.client

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.core.store.Dataset
import filodb.spark.Filo
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

object FiloExecutor {
  /** handles the show tables action
    * @param input  the statement to be parsed for the show tables command
    */
  def handleShow(input: String, sql: SQLContext, sc: SparkContext, dfFailure: DataFrame): DataFrame = {
    if (SimpleParser.parseShow(input)) {
      val description = Filo.metaStore.projectionTable.getAllSuperProjectionNames
      val result = for {
        infoAll <- description
      } yield {
        val seq: Seq[String] = infoAll.map(name =>
          s"""{"dataset" : "$name"}""")
        sql.read.json(sc.parallelize(seq))
      }
      Filo.parse(result)(df => df)
    }
    else {
        dfFailure
    }
  }

  /** handles the create table action
    * @param create  the case class for describe consisting the parameters for create command
    */
  def handleCreate(create: Create, sql: SQLContext, dfSuccess: DataFrame): DataFrame = {
    val columns = create.columns map {
      case (colName, colType) =>
        Column(colName, create.tableName, 0, ColumnType.toFiloColumnType(colType))
    } toSeq
    val dataset = Dataset.apply(create.tableName, columns,
      create.partitionCols, create.primaryCols, create.segmentCols)
    Filo.metaStore.addProjection(dataset.projectionInfoSeq.head)
    /* val tableDS = sql.read.format("filodb.spark").option("dataset", create.tableName).load()
    tableDS.registerTempTable(create.tableName) */
    dfSuccess
  }

  /** handles the describe tables or projections action
    * @param describe the case class for describe consisting the parameters for describe command
    */
  def handleDescribe(describe: Describe, sql: SQLContext, sc: SparkContext, dfFailure: DataFrame): DataFrame = {
    if (describe.isTable) {
      val description = Filo.metaStore.projectionTable.getSuperProjection(describe.tableName)
      val result = for {
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
      Filo.parse(result)(df => df)
    }
    else {
      describe.projectionName match {
        case Some(id) =>
          val description = Filo.metaStore.getProjection(describe.tableName, id)
          val result = for {
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
          Filo.parse(result)(df => df)
        case _ =>
          dfFailure
      }
    }

  }
}
