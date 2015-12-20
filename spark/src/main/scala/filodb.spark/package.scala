package filodb

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.metadata.Column
import filodb.core.reprojector.Reprojector
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

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
                          flushSize: Int = 10000,
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

      // For each partition, start the ingestion
      df.rdd.mapPartitions { rowIter =>
        Filo.init(filoConfig)
        Filo.parse(ingest(flushSize, dataset, rowIter, dfOrderSchema))(r => r).iterator
      }.count()
    }


    private def ingest(flushSize: Int, dataset: String, rowIter: Iterator[Row], dfOrderSchema: Seq[Column]) = {
      implicit val executionContext = Filo.executionContext
      val datasetObj = Filo.getDatasetObj(dataset)
      Future sequence datasetObj.projections.flatMap { projection =>
        // group the flushes into flush Size rows
        // this is a compromise between memory and ingestion speed.
        rowIter.grouped(flushSize).map { rows =>
          // reproject the data
          val projectedData = Reprojector.project(projection,
            rows.map(r => RddRowReader(r)).iterator,
            Some(dfOrderSchema)
          )
          // now flush each segment
          projectedData.flatMap { case (partition, segmentFlushes) =>
            segmentFlushes
              .map(flush => Filo.columnStore.flushToSegment(flush))
          }
        }

      }.flatten

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
