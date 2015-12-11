package filodb

import filodb.core.reprojector.Reprojector
import filodb.core.store.Dataset
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import scala.concurrent.Future

package object spark {
  implicit val executionContext = Filo.executionContext
  implicit class FiloContext(sqlContext: SQLContext) {

    def saveAsFiloDataset(df: DataFrame,
                          dataset: String,
                          mode: SaveMode = SaveMode.Append): Unit = {
      Filo.init(sqlContext.sparkContext)
      val dfColumns = df.schema.map(_.name)
      val numPartitions = df.rdd.partitions.size
      ingestDF(df, dataset, dfColumns)
    }

    private def ingestDF(df: DataFrame, dataset: String, dfColumns: Seq[String]): Unit =
    // For each partition, start the ingestion
      df.rdd.mapPartitions{  rowIter =>
        // init the Filo setup locally on the node.
        Filo.init(df.sqlContext.sparkContext)
        val datasetObj = FiloRelation.getDatasetObj(dataset)
        Filo.parse(ingest(datasetObj, rowIter))(r => r).iterator
      }.collect()


    private def ingest(dataset: Dataset, rowIter: Iterator[Row]) =
      Future sequence dataset.projections.flatMap { projection =>
        // reproject the data
        val projectedData = Reprojector.project(projection, rowIter.map(r => RddRowReader(r)).toSeq)
        projectedData.map { case (partition, segmentFlushes) =>
          segmentFlushes
            .map(flush => Filo.columnStore.flushToSegment(flush))
        }
      }.flatten

  }

}
