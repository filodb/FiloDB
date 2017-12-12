package filodb.coordinator

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import kamon.Kamon
import kamon.metric.Entity
import kamon.metric.instrument.{Counter, Histogram}
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import net.ceedubs.ficus.Ficus._

object KamonLogger {
  def props: Props = Props(classOf[KamonLogger])

  /**
   * Starts the Kamon logger actor, subscribing various metrics to it based on config
   * @param system the ActorSystem from which to start the actor
   * @param config the Config for determining which metrics to log
   *
   * ==Configuration==
   * {{{
   *   enabled = true
   *   trace-segments = "**"   # Make this the empty string to disable.  Default value is ** everything
   * }}}
   */
  def start(system: ActorSystem, config: Config): Option[ActorRef] = {
    if (config.getBoolean("enabled")) {
      val subscriber = system.actorOf(props, "kamon-logger-subscriber")

      def subscribe(category: String): Unit =
        Kamon.metrics.subscribe(category, config.as[Option[String]](category).getOrElse("**"), subscriber)

      subscribe("trace")
      subscribe("trace-segment")
      subscribe("counter")
      subscribe("histogram")
      subscribe("min-max-counter")
      subscribe("gauge")

      Some(subscriber)
    } else {
      None
    }
  }
}

/**
 * An actor that logs all Kamon metrics
 *
 * TODO: see if we could merge the metrics together and log them less often.  Instrument has a merge method.
 */
class KamonLogger extends BaseActor {
  def receive: Receive = {
    case tick: TickMetricSnapshot =>
      tick.metrics foreach {
        case (Entity(name, "trace", tags), snapshot)               =>
          logHistogram(name, snapshot.histogram("elapsed-time").get, "trace", tags)
        case (Entity(name, "trace-segment", tags), snapshot)       =>
          logHistogram(name, snapshot.histogram("elapsed-time").get, "trace-segment", tags)
        case (Entity(name, "histogram", tags), snapshot)           =>
          logHistogram(name, snapshot.histogram("histogram").get, "histogram", tags)
        case (Entity(name, "counter", tags), snapshot)             =>
          logCounter(name, snapshot.counter("counter").get, tags)
        case (Entity(name, "min-max-counter", tags), snapshot)     =>
          logMinAvgMax(name, snapshot.minMaxCounter("min-max-counter").get, "min-max", tags)
        case (Entity(name, "gauge", tags), snapshot)               =>
          logMinAvgMax(name, snapshot.gauge("gauge").get, "gauge", tags)
        case x: Any                                                =>
      }
  }

  def logCounter(name: String, c: Counter.Snapshot, tags: Map[String, String]): Unit =
    logger.info(s"KAMON counter name=$name ${formatTags(tags)} count=${c.count}")

  def logHistogram(name: String, h: Histogram.Snapshot, category: String = "histogram",
                   tags: Map[String, String]): Unit =
    logger.info(s"KAMON $category name=$name ${formatTags(tags)} n=${h.numberOfMeasurements} min=${h.min} " +
                s"p50=${h.percentile(50.0D)} p90=${h.percentile(90.0D)} " +
                s"p95=${h.percentile(95.0D)} p99=${h.percentile(99.0D)} " +
                s"p999=${h.percentile(99.9D)} max=${h.max}")

  private def average(histogram: Histogram.Snapshot): Double = {
    if (histogram.numberOfMeasurements == 0) { 0D }
    else { histogram.sum / histogram.numberOfMeasurements }
  }

  def logMinAvgMax(name: String, h: Histogram.Snapshot, category: String, tags: Map[String, String]): Unit =
    logger.info(s"KAMON $category name=$name " +
                s"${formatTags(tags)} " +
                s"min=${h.min} " +
                s"avg=${average(h)} " +
                s"max=${h.max}")

  private def formatTags(tags: Map[String, String]) = tags.view.map { case (k,v) => s"$k=$v" }.mkString(" ")
}