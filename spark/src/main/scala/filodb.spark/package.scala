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
                          mode: SaveMode = SaveMode.Append): Unit = {
      val datasetObj = FiloRelation.getDatasetObj(dataset)
      val namesTypes = df.schema.map { f => f.name -> f.dataType }
      val schema = datasetObj.schema.map(col => col.name -> col)
      validateSchema(namesTypes, schema)
      val schemaMap = schema.toMap
      val dfOrderSchema = namesTypes.map { case (colName, x) =>
        schemaMap(colName)
      }

      val filoConfig = configFromSpark(sqlContext.sparkContext)
      // For each partition, start the ingestion
      df.rdd.mapPartitions { rowIter =>
        Filo.parse(ingest(filoConfig, dataset, rowIter, dfOrderSchema))(r => r).iterator
      }.collect()
    }


    private def ingest(filoConfig: Config, dataset: String, rowIter: Iterator[Row], dfOrderSchema: Seq[Column]) = {
      Filo.init(filoConfig)
      implicit val executionContext = Filo.executionContext
      val datasetObj = FiloRelation.getDatasetObj(dataset)
      Future sequence datasetObj.projections.flatMap { projection =>
        // reproject the data
        val projectedData = Reprojector.project(projection,
          rowIter.map(r => RddRowReader(r)).toSeq,
          Some(dfOrderSchema)
        )
        projectedData.map { case (partition, segmentFlushes) =>
          segmentFlushes
            .map(flush => Filo.columnStore.flushToSegment(flush))
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
