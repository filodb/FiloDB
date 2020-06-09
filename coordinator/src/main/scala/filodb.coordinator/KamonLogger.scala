package filodb.coordinator

import scala.concurrent.Await

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.metric.{MeasurementUnit, MetricSnapshot, PeriodSnapshot}
import kamon.metric.MeasurementUnit.{information, time}
import kamon.metric.MeasurementUnit.Dimension.{Information, Time}
import kamon.module.{MetricReporter, Module, ModuleFactory, SpanReporter}
import kamon.tag.TagSet
import kamon.trace.Span
import kamon.util.Clock
import kamon.Kamon

class KamonMetricsLogReporter extends MetricReporter with StrictLogging {

  logger.info("Started the KamonMetricsLog reporter")

  override def stop(): Unit = {}

  override def reconfigure(config: Config): Unit = {}

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    logMetricCounters(snapshot.counters, "counter")
    logMetricGauge(snapshot.gauges, "gauge")
    logMetricDistribution(snapshot.histograms, "histograms")
    logMetricDistribution(snapshot.timers, "timers")
    logMetricDistribution(snapshot.rangeSamplers, "rangeSamplers")
  }

  private def logMetricCounters(metrics: Seq[MetricSnapshot.Values[Long]], metricType: String): Unit = {
    for { c <- metrics } {
      val name = normalizeMetricName(c.name, c.settings.unit)
      c.instruments.foreach(instrument => {
        val value = scale(instrument.value, c.settings.unit)
        logger.debug(s"KAMON ${metricType} name=${name} ${formatTags(instrument.tags)} value=$value")
      })
    }
  }

  private def logMetricGauge(metrics: Seq[MetricSnapshot.Values[Double]], metricType: String): Unit = {
    for { c <- metrics } {
      val name = normalizeMetricName(c.name, c.settings.unit)
      c.instruments.foreach(instrument => {
        val value = scale(instrument.value, c.settings.unit)
        logger.debug(s"KAMON ${metricType} name=${name} ${formatTags(instrument.tags)} value=$value")
      })
    }
  }

  private def logMetricDistribution(metrics: Seq[MetricSnapshot.Distributions], metricType: String): Unit = {
    for { m <- metrics } {
      val name = normalizeMetricName(m.name, m.settings.unit)

      m.instruments.foreach(instrument => {
        val h = instrument.value
        def percentile(percentile: Double) = scale(h.percentile(25.0D).value, m.settings.unit)
        if(h.count > 0) {
          logger.debug(s"KAMON ${metricType} name=$name ${formatTags(instrument.tags)} " +
            s"n=${h.count} sum=${h.sum} min=${scale(h.min, m.settings.unit)} " +
            s"p50=${percentile(50.0D)} p90=${percentile(90.0D)} " +
            s"p95=${percentile(95.0D)} p99=${percentile(99.0D)} " +
            s"p999=${percentile(99.9D)} max=${scale(h.max, m.settings.unit)}")
        }
      })
    }
  }

  private def formatTags(tags: TagSet) = tags.iterator(tagPair => tagPair.toString)
    .map{case t => s"${t.key}=${t.value}"}.mkString(" ")

  private def normalizeLabelName(label: String): String =
    label.map(charOrUnderscore)

  private def charOrUnderscore(char: Char): Char =
    if (char.isLetterOrDigit || char == '_') char else '_'

  private def normalizeMetricName(metricName: String, unit: MeasurementUnit): String = {
    val normalizedMetricName = metricName.map(charOrUnderscore)
    unit.dimension match  {
      case Time         => normalizedMetricName + "_seconds"
      case Information  => normalizedMetricName + "_bytes"
      case _            => normalizedMetricName
    }
  }

  private def scale(value: Double, unit: MeasurementUnit): Double = unit.dimension match {
    case Time if unit.magnitude != time.seconds.magnitude =>
      MeasurementUnit.convert(value, unit, time.seconds)
    case Information  if unit.magnitude != information.bytes.magnitude  =>
      MeasurementUnit.convert(value, unit, information.bytes)
    case _ => value
  }
}

class KamonSpanLogReporter extends SpanReporter with StrictLogging {

  logger.info("Started the KamonSpanLog reporter")

  override def reportSpans(spans: Seq[Span.Finished]): Unit = {
    spans.groupBy(_.operationName).foreach { case (name, spans) =>
      val durations = spans.map { s => Math.floorDiv(Clock.nanosBetween(s.from, s.to), 1000) }
      logger.debug(s"KAMON-TRACE name $name min=${durations.min} max=${durations.max} " +
        s"avg=${durations.sum.toFloat / durations.size}")
    }
  }

  override def stop(): Unit =
    logger.info("Stopped the Zipkin reporter")

  override def reconfigure(config: Config): Unit = {}
}

object KamonLogger {
  class MetricsLogFactory extends ModuleFactory {
    override def create(settings: ModuleFactory.Settings): Module =
      new KamonMetricsLogReporter
  }
  class SpanLogFactory extends ModuleFactory {
    override def create(settings: ModuleFactory.Settings): Module =
      new KamonSpanLogReporter
  }
}

object KamonShutdownHook extends StrictLogging {

  import scala.concurrent.duration._

  private val shutdownHookAdded = new java.util.concurrent.atomic.AtomicBoolean(false)
  def registerShutdownHook(): Unit = {
    if (shutdownHookAdded.compareAndSet(false, true)) {
      logger.info(s"Registering Kamon Shutdown Hook...")
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          logger.info(s"Stopping Kamon modules - this will ensure that last few metrics are drained")
          try {
            Await.result(Kamon.stopModules(), 5.minutes)
            logger.info(s"Finished stopping Kamon modules")
          } catch { case e: Exception =>
              logger.error(s"Exception when stopping Kamon Modules", e)
          }
        }
      })
    }
  }
}