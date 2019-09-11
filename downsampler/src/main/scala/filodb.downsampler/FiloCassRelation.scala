package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ArrayType, BinaryType, StructField, StructType}

import filodb.core.DatasetRef
import filodb.core.store.RawPartData

class FiloCassRelation(@transient override val sqlContext: SQLContext) extends BaseRelation with StrictLogging {

  import filodb.core.Iterators._

  val schema = PartDataRow.schema

  /**
    * Builds an RDD of batched raw partition data that belong to the specified user time range
    * for time series that were ingested in the given period. Returned RDD will have batches
    * of `PartDataRow`. Batches will be of maximum size `batchSize`
    */
  def buildScan(dataset: DatasetRef,
                ingestTimeStart: Long,
                ingestTimeEnd: Long,
                userStartTime: Long,
                userEndTime: Long,
                batchSize: Int): RDD[Seq[PartDataRow]] = {

    val splits = PerSparkExecutorState.cassandraColStore.getScanSplits(dataset)
    sqlContext
      .sparkContext
      .makeRDD(splits)
      .mapPartitions { splitIter =>
        val rawDataSource = PerSparkExecutorState.cassandraColStore
        rawDataSource.getChunksByIngestionTimeRange(dataset, splitIter,
                                                    ingestTimeStart, ingestTimeEnd,
                                                    userStartTime, userEndTime, batchSize)
                     .toIterator()
                     .map { p => p.map(PartDataRow(_)) }
      }
  }
}

case class PartDataRow(partData: RawPartData) extends Row {
  override def length: Int = 1
  override def get(i: Int): Any = if (i ==0) partData else throw new IllegalArgumentException(s"invalid column $i")
  override def copy(): Row = PartDataRow(partData)
}

object PartDataRow {
  val schema = StructType(
    Seq(StructField("partKey", BinaryType, false),
      StructField("chunkSetInfo", BinaryType, false),
      StructField("chunkSetVectors", ArrayType(BinaryType), false))
  )
}
