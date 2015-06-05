package filodb

import com.typesafe.config.Config
import org.apache.spark.sql.{SQLContext, DataFrame}

package object spark {
  implicit class FiloContext(sqlContext: SQLContext) {
    def filoDataset(filoConfig: Config,
                    dataset: String,
                    version: Int = 0,
                    minPartitions: Int = FiloRelation.DefaultMinPartitions): DataFrame =
      sqlContext.baseRelationToDataFrame(FiloRelation(filoConfig, dataset, version, minPartitions)(sqlContext))
  }
}