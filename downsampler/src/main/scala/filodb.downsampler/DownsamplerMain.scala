package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

/**
  *
  * Goal: Downsample all real-time data.
  * Goal: Align chunks when this job is run in multiple DCs so that cross-dc repairs can be done.
  * Non-Goal: Downsampling of non-real time data or data with different epoch.
  *
  * Strategy is to run this spark job every 6 hours at 8am, 2pm, 8pm, 2am each day.
  *
  * Run at 8am: We query data with ingestionTime from 10pm to 8am.
  *             Then query and downsample data with userTime between 12am to 6am.
  *             Downsampled chunk would have an ingestionTime of 12am.
  * Run at 2pm: We query data with ingestionTime from 8am to 2pm.
  *             Then query and downsample data with userTime between 6am to 12pm.
  *             Downsampled chunk would have an ingestionTime of 6am.
  * Run at 8pm: We query data with ingestionTime from 10am to 8pm.
  *             Then query and downsample data with userTime between 12pm to 6pm.
  *             Downsampled chunk would have an ingestionTime of 12pm.
  * Run at 2am: We query data with ingestionTime from 8pm to 2am.
  *             Then query and downsample data with userTime between 6pm to 12am.
  *             Downsampled chunk would have an ingestionTime of 6pm.
  *
  * This will cover all data with userTime 12am to 12am.
  * Since we query for a broader ingestionTime, it will include data arriving early/late by 2 hours.
  *
  */
object DownsamplerMain extends App with StrictLogging {

  import DownsamplerSettings._

  private val spark = SparkSession.builder()
    .appName("FiloDBDownsampler")
    .getOrCreate()

  val splits = BatchDownsampler.cassandraColStore.getScanSplits(BatchDownsampler.rawDatasetRef)

  spark.sparkContext
    .makeRDD(splits)
    .mapPartitions { splitIter =>
      import filodb.core.Iterators._
      val rawDataSource = BatchDownsampler.cassandraColStore
      rawDataSource.getChunksByIngestionTimeRange(BatchDownsampler.rawDatasetRef, splitIter,
                                                  ingestionTimeStart, ingestionTimeEnd,
                                                  userTimeStart, userTimeEnd, batchSize).toIterator()
    }
    .foreach { rawPartsBatch =>
      BatchDownsampler.downsampleBatch(rawPartsBatch)
    }
  BatchDownsampler.cassandraColStore.shutdown()
  spark.sparkContext.stop()

  // TODO migrate index entries

}
