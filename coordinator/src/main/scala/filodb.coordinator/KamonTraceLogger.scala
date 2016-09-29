package filodb.coordinator

import akka.actor.Props
import com.typesafe.config.Config
import kamon.trace.TraceInfo
import org.joda.time.DateTime

object KamonTraceLogger {
  def props(config: Config): Props = Props(classOf[KamonTraceLogger], config)
}

/**
 * An actor that receives trace messages from Kamon and logs them.
 * In the future this could be open sourced separately or contributed back to Kamon.
 * @param config a Config within the filodb scope, eg globalConfig.getConfig("filodb")
 */
class KamonTraceLogger(config: Config) extends BaseActor {
  def receive: Receive = {
    case t: TraceInfo =>
      val dt = new DateTime(t.timestamp.nanos / 1000000L)
      logger.info(s"KAMON-TRACE name=${t.name} timestamp=$dt elapsedTime=${t.elapsedTime}")
      t.segments.foreach { seg =>
        logger.info(s" >> +[${seg.timestamp - t.timestamp}] name=${seg.name} category=${seg.category} " +
                    s"elapsed=${seg.elapsedTime}")
      }
  }
}