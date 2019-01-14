package filodb.core.downsample

import scala.concurrent.Future

import filodb.core.{Response, Success}

trait DownsamplePublisher {
  def start(): Unit
  def stop(): Unit
  def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response]
}

object NoOpPublisher extends DownsamplePublisher {
  override def publish(shardNum: Int, resolution: Int,
                       records: Seq[Array[Byte]]): Future[Response] = Future.successful(Success)

  override def start(): Unit = {}

  override def stop(): Unit = {}
}
