package filodb

import com.typesafe.config.Config
import filodb.core.reprojector.Reprojector
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import scala.concurrent.Future

package object spark {

  implicit class FiloContext(sqlContext: SQLContext) extends Serializable {

    def saveAsFiloDataset(df: DataFrame,
                          dataset: String,
                          mode: SaveMode = SaveMode.Append): Unit = {
      val filoConfig = Filo.configFromSpark(sqlContext.sparkContext)
      // For each partition, start the ingestion
      df.rdd.mapPartitions { rowIter =>
        Filo.parse(ingest(filoConfig, dataset, rowIter))(r => r).iterator
      }.collect()
    }

    private def ingest(filoConfig: Config, dataset: String, rowIter: Iterator[Row]) = {
      Filo.init(filoConfig)
      implicit val executionContext = Filo.executionContext
      val datasetObj = FiloRelation.getDatasetObj(dataset)
      Future sequence datasetObj.projections.flatMap { projection =>
        // reproject the data
        val projectedData = Reprojector.project(projection, rowIter.map(r => RddRowReader(r)).toSeq)
        projectedData.map { case (partition, segmentFlushes) =>
          segmentFlushes
            .map(flush => Filo.columnStore.flushToSegment(flush))
        }
      }.flatten

    }

  }

}
