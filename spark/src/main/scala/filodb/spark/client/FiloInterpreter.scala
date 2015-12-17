package filodb.spark.client

import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.spark._
import filodb.core.store.Dataset
import filodb.spark.Filo
import org.apache.spark.sql.SQLContext
import scala.language.postfixOps

object FiloInterpreter {

  def interpret(input: String, sql: SQLContext): Any = {
    input.toLowerCase.trim match {

      case s: String if s.startsWith("s") =>
        if (SimpleParser.parseSelect(input)) {
          val df = sql.sql(input)
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
        Filo.metaStore.addProjection(dataset.projectionInfoSeq.head)

      case l: String if l.startsWith("l") =>
        val load = SimpleParser.parseLoad(input)
        val dataDF = sql.read.format(load.format).options(load.options).load(load.url)
        sql.saveAsFiloDataset(dataDF, load.tableName)

      case _ => throw new IllegalArgumentException("Cannot parse the given statement")
    }
  }
}
