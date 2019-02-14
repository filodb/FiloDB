package filodb.core.downsample

import scala.concurrent.Future

import com.typesafe.scalalogging.StrictLogging

import filodb.core.{Response, Success}

/**
  * Dispatches downsample data to the FiloDB datasets holding downsampled data.
  */
trait DownsamplePublisher {
  /**
    * Start the downsample publish pipeline/thread
    */
  def start(): Unit

  /**
    * Cleanly stop the downsample publish task. Typically called on shutdown.
    */
  def stop(): Unit

  /**
    * Dispatch samples to the downsampling dataset.
    * The publisher needs to take care of retry logic, and acks if any.
    * Flush pipeline in TimeSeriesShard is expected to move on and not hold off the ingestion pipeline
    * if there is any slowdown here.
    *
    * For now, there is a possibility of loss of samples in memory that have not been dispatched.
    *
    * @param shardNum
    * @param resolution
    * @param records each Array of Byte is a bunch of Binary Records that can be deserialized by the ingestion pipeline.
    * @return Future of Success if all is good.
    */
  def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response]
}

/**
  * Typically used when downsampling is disabled.
  */
object NoOpDownsamplePublisher extends DownsamplePublisher with StrictLogging {
  override def publish(shardNum: Int, resolution: Int,
                       records: Seq[Array[Byte]]): Future[Response] = Future.successful(Success)

  override def start(): Unit = {
    logger.info("Starting NoOpDownsamplePublisher since downsampling is disabled")
  }

  override def stop(): Unit = {
    logger.info("Stopping NoOpDownsamplePublisher since downsampling is disabled")
  }
}
