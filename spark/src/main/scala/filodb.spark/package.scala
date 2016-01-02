package filodb

import com.typesafe.config.{Config, ConfigFactory}
import filodb.coordinator.reactive.StreamingProcessor
import filodb.core.metadata.{Column, Projection}
import filodb.core.reprojector.Reprojector
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.velvia.filo.RowReader

import scala.concurrent.Future


package spark {

case class DatasetNotFound(dataset: String) extends Exception(s"Dataset $dataset not found")

// For each mismatch: the column name, DataFrame type, and existing column type
case class ColumnTypeMismatch(mismatches: Set[(String, DataType, Column.ColumnType)]) extends Exception

case class NoSortColumn(name: String) extends Exception(s"No sort column found $name")

case class NoPartitionColumn(name: String) extends Exception(s"No partition column found $name")

case class BadSchemaError(reason: String) extends Exception(reason)

}

package object spark {


  def configFromSpark(context: SparkContext): Config = {
    val conf = context.getConf
    val filoOverrides = conf.getAll.collect { case (k, v) if k.startsWith("spark.filodb") =>
      k.replace("spark.filodb.", "") -> v
    }
    import scala.collection.JavaConverters._
    ConfigFactory.parseMap(filoOverrides.toMap.asJava)
  }


  implicit class FiloContext(sqlContext: SQLContext) extends Serializable {

    import TypeConverters._

    def saveAsFiloDataset(df: DataFrame,
                          dataset: String,
                          flushSize: Int = 1000,
                          mode: SaveMode = SaveMode.Append): Unit = {
      val filoConfig = configFromSpark(sqlContext.sparkContext)
      Filo.init(filoConfig)
      val datasetObj = Filo.getDatasetObj(dataset)
      val namesTypes = df.schema.map { f => f.name -> f.dataType }
      val schema = datasetObj.schema.map(col => col.name -> col)
      validateSchema(namesTypes, schema)
      val schemaMap = schema.toMap
      val dfOrderSchema = namesTypes.map { case (colName, x) =>
        schemaMap(colName)
      }
      // for each of the projections
      datasetObj.projections.map { projection =>
        // For each partition, start the ingestion
        df.rdd.mapPartitions { rowIter =>
          Filo.init(filoConfig)
          ingest(flushSize, projection, rowIter, dfOrderSchema)
          Iterator.empty
        }.count()

      }
    }

    private def ingest(flushSize: Int, projection: Projection, rowIter: Iterator[Row], dfOrderSchema: Seq[Column]) = {
      implicit val executionContext = Filo.executionContext

      def save(rows: Iterator[Row]) = Filo.parse(
        Future sequence Reprojector.project(projection,
          new RowIterator(rows),
          Some(dfOrderSchema)
        ).map { flush =>
          Filo.columnStore.flushToSegment(flush)
        })(r => r)

      val rowProcessorProps = StreamingProcessor.props[Row, Iterator[Boolean]](
        rowIter, save, Filo.memoryCheck(512))

      val rowProcessor = Filo.newActor(rowProcessorProps)
      StreamingProcessor.start(rowProcessor)
    }


    class RowIterator(iterator: Iterator[Row]) extends Iterator[RowReader] {
      val rowReader = new RddRowReader

      override def hasNext: Boolean = iterator.hasNext

      override def next(): RowReader = {
        rowReader.row = iterator.next
        rowReader
      }
    }

    private def validateSchema(namesTypes: Seq[(String, DataType)], schema: Seq[(String, Column)]): Unit = {
      val dfNamesMap = namesTypes.toMap
      val schemaMap = schema.toMap

      val matchingCols = namesTypes.toMap.keySet.intersect(schema.toMap.keySet)
      // Type-check matching columns
      val matchingTypeErrs = matchingCols.collect {
        case colName: String if sqlTypeToColType(dfNamesMap(colName)) != schemaMap(colName).columnType =>
          (colName, dfNamesMap(colName), schemaMap(colName).columnType)
      }
      if (matchingTypeErrs.nonEmpty) throw ColumnTypeMismatch(matchingTypeErrs)
    }
  }

}
