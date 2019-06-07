package filodb.coordinator

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.{MetricReporter, SpanReporter}
import kamon.metric._
import kamon.metric.MeasurementUnit.{information, time}
import kamon.metric.MeasurementUnit.Dimension.{Information, Time}
import kamon.trace.Span
import kamon.util.Clock

class KamonMetricsLogReporter extends MetricReporter with StrictLogging {
  override def start(): Unit = {
    logger.info("Started KamonMetricsLogReporter successfully")
  }

  override def stop(): Unit = {}

  override def reconfigure(config: Config): Unit = {}

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    logMetricValues(snapshot.metrics.counters, "counter")
    logMetricValues(snapshot.metrics.gauges, "gauge")
    logMetricDistribution(snapshot.metrics.histograms, "histograms")
    logMetricDistribution(snapshot.metrics.rangeSamplers, "rangeSamplers")
  }

  private def logMetricValues(metrics: Seq[MetricValue], metricType: String): Unit = {
    for { c <- metrics } {
      val name = normalizeMetricName(c.name, c.unit)
      val value = scale(c.value, c.unit)
      logger.info(s"KAMON ${metricType} name=${name} ${formatTags(c.tags)} value=$value")
    }
  }

  private def logMetricDistribution(metrics: Seq[MetricDistribution], metricType: String): Unit = {
    for { m <- metrics } {
      val name = normalizeMetricName(m.name, m.unit)
      val h = m.distribution
      def percentile(percentile: Double) = scale(h.percentile(25.0D).value, m.unit)
      logger.info(s"KAMON ${metricType} name=$name ${formatTags(m.tags)} " +
        s"n=${h.count} min=${scale(h.min, m.unit)} " +
        s"p50=${percentile(50.0D)} p90=${percentile(90.0D)} " +
        s"p95=${percentile(95.0D)} p99=${percentile(99.0D)} " +
        s"p999=${percentile(99.9D)} max=${scale(h.max, m.unit)}")
    }
  }

  private def formatTags(tags: Map[String, String]) = tags.view.map { case (k, v) => s"$k=$v" }.mkString(" ")

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

  private def scale(value: Long, unit: MeasurementUnit): Double = unit.dimension match {
    case Time if unit.magnitude != time.seconds.magnitude =>
      MeasurementUnit.scale(value, unit, time.seconds)
    case Information  if unit.magnitude != information.bytes.magnitude  =>
      MeasurementUnit.scale(value, unit, information.bytes)
    case _ => value
  }

}

class KamonSpanLogReporter extends SpanReporter with StrictLogging {

  override def reportSpans(spans: Seq[Span.FinishedSpan]): Unit = {
    spans.groupBy(_.operationName).foreach { case (name, spans) =>
      val durations = spans.map { s => Math.floorDiv(Clock.nanosBetween(s.from, s.to), 1000) }
      logger.info(s"KAMON-TRACE name $name min=${durations.min} max=${durations.max} " +
        s"avg=${durations.sum.toFloat / durations.size}")
    }
  }

  override def start(): Unit = {
    logger.info("Started KamonSpanLogReporter successfully")
  }

  override def stop(): Unit = {}

  override def reconfigure(config: Config): Unit = {}
}