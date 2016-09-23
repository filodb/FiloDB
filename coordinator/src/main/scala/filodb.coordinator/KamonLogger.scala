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
        // case (Entity(name, "akka-actor", _), snapshot)       =>
        // case (Entity(name, "akka-router", _), snapshot)      =>
        // case (Entity(name, "akka-dispatcher", _), snapshot)  =>
        // case (Entity(name, "executor-service", _), snapshot) =>
        case (Entity(name, "trace", _), snapshot)               =>
          logHistogram(name, snapshot.histogram("elapsed-time").get, "trace")
        case (Entity(name, "trace-segment", _), snapshot)       =>
          logHistogram(name, snapshot.histogram("elapsed-time").get, "trace-segment")
        case (Entity(name, "histogram", _), snapshot)           =>
          logHistogram(name, snapshot.histogram("histogram").get)
        case (Entity(name, "counter", _), snapshot)             =>
          logCounter(name, snapshot.counter("counter").get)
        case (Entity(name, "min-max-counter", _), snapshot)     =>
          logMinAvgMax(name, snapshot.minMaxCounter("min-max-counter").get, "min-max")
        case (Entity(name, "gauge", _), snapshot)               =>
          logMinAvgMax(name, snapshot.gauge("gauge").get, "gauge")
        case x: Any                                             =>
      }
  }

  def logCounter(name: String, c: Counter.Snapshot): Unit =
    logger.info(s"KAMON counter name=$name count=${c.count}")

  def logHistogram(name: String, h: Histogram.Snapshot, category: String = "histogram"): Unit =
    logger.info(s"KAMON $category name=$name n=${h.numberOfMeasurements} min=${h.min} " +
                s"p50=${h.percentile(50.0D)} p90=${h.percentile(90.0D)} " +
                s"p95=${h.percentile(95.0D)} p99=${h.percentile(99.0D)} " +
                s"p999=${h.percentile(99.9D)} max=${h.max}")

  private def average(histogram: Histogram.Snapshot): Double = {
    if (histogram.numberOfMeasurements == 0) { 0D }
    else { histogram.sum / histogram.numberOfMeasurements }
  }

  def logMinAvgMax(name: String, h: Histogram.Snapshot, category: String): Unit =
    logger.info(s"KAMON $category name=$name min=${h.min} avg=${average(h)} max=${h.max}")
}