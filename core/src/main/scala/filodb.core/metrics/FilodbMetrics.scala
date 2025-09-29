package filodb.core.metrics

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics._
import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingMetricExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.instrumentation.oshi.SystemMetrics
import io.opentelemetry.instrumentation.runtimemetrics.java8.{Classes, Cpu, GarbageCollector, MemoryPools, Threads}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.{Aggregation, InstrumentSelector, InstrumentType, SdkMeterProvider, View}
import io.opentelemetry.sdk.metrics.data.AggregationTemporality
import io.opentelemetry.sdk.metrics.export.{AggregationTemporalitySelector, MetricExporter, PeriodicMetricReader}
import io.opentelemetry.sdk.resources.Resource
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import net.ceedubs.ficus.Ficus._

import filodb.core.GlobalConfig

/**
 * Configuration for OpenTelemetry metrics
 */
case class OTelMetricsConfig(exportIntervalSeconds: Int = 60,
                             resourceAttributes: Map[String, String],
                             otlpHeaders: Map[String, String],
                             exporterType: String, // "otlp" or "log"
                             exponentialHistogram: Boolean,
                             customHistogramBucketsTime: List[Double], // used only if exponentialHistogram=false
                             customHistogramBuckets: List[Double], // used only if exponentialHistogram=false
                             otlpEndpoint: Option[String],
                             otlpTrustedCertsPath: Option[String],
                             otlpClientCertPath: Option[String],
                             otlpClientKeyPath: Option[String])

object OTelMetricsConfig {
  def fromConfig(metricsConfig: Config): OTelMetricsConfig = {

    OTelMetricsConfig(
      otlpEndpoint = metricsConfig.as[Option[String]]("otlp-endpoint"),
      exportIntervalSeconds = metricsConfig.as[Int]("export-interval-seconds"),
      resourceAttributes = metricsConfig.as[Map[String, String]]("resource-attributes"),
      exporterType = metricsConfig.as[String]("exporter-type"),
      exponentialHistogram = metricsConfig.as[Boolean]("exponential-histogram"),
      customHistogramBucketsTime = metricsConfig.as[List[Double]]("custom-histogram-buckets-time").sorted,
      customHistogramBuckets = metricsConfig.as[List[Double]]("custom-histogram-buckets").sorted,
      // otlp specific settings
      otlpHeaders = metricsConfig.as[Map[String, String]]("otlp-headers"),
      otlpTrustedCertsPath = metricsConfig.as[Option[String]]("otlp-trusted-certs-path"),
      otlpClientCertPath = metricsConfig.as[Option[String]]("otlp-client-cert-path"),
      otlpClientKeyPath = metricsConfig.as[Option[String]]("otlp-client-key-path")
    )
  }
}

/**
 * Wrapper for OpenTelemetry instruments with additional key-value pairs
 */
sealed trait MetricsInstrument {
  def baseAttributes: Attributes
  def withAttributes(additionalAttributes: Map[String, String]): Attributes = {
    val builder = baseAttributes.toBuilder
    additionalAttributes.foreach { case (key, value) =>
      builder.put(AttributeKey.stringKey(key), value)
    }
    builder.build()
  }
}

case class MetricsCounter(otelCounter: Option[LongCounter],
                          kamonCounter: Option[kamon.metric.Counter],
                          baseAttributes: Attributes) extends MetricsInstrument {
  def increment(value: Long = 1, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    otelCounter.foreach(_.add(value, withAttributes(additionalAttributes)))
    if (additionalAttributes.nonEmpty) {
      // Kamon withTags creates a new instrument each time, so only do this if there are additional attributes
      kamonCounter.foreach(_.withTags(TagSet.from(additionalAttributes)).increment(value))
    } else {
      kamonCounter.foreach(_.increment(value))
    }
  }
}

case class MetricsUpDownCounter(otelCounter: Option[LongUpDownCounter],
                                kamonCounter: Option[kamon.metric.Gauge],
                                baseAttributes: Attributes) extends MetricsInstrument {
  def increment(value: Long = 1, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    otelCounter.foreach(_.add(value, withAttributes(additionalAttributes)))
    if (additionalAttributes.nonEmpty) {
      // Kamon withTags creates a new instrument each time, so only do this if there are additional attributes
      kamonCounter.foreach(_.withTags(TagSet.from(additionalAttributes)).increment(value))
    } else {
      kamonCounter.foreach(_.increment(value))
    }
  }
}

case class MetricsGauge(otelGauge: Option[DoubleGauge],
                        kamonGauge: Option[kamon.metric.Gauge],
                        baseAttributes: Attributes) extends MetricsInstrument {
  def update(value: Double, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    otelGauge.foreach(_.set(value, withAttributes(additionalAttributes)))
    if (additionalAttributes.nonEmpty) {
      // Kamon withTags creates a new instrument each time, so only do this if there are additional attributes
      kamonGauge.foreach(_.withTags(TagSet.from(additionalAttributes)).update(value))
    } else {
      kamonGauge.foreach(_.update(value))
    }
  }
}

case class MetricsHistogram(otelHistogram: Option[DoubleHistogram],
                            kamonHistogram: Option[kamon.metric.Histogram],
                            timeUnit: Option[TimeUnit],
                            baseAttributes: Attributes) extends MetricsInstrument {
  def record(value: Long, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    val valueInSeconds = timeUnit match {
      case Some(unit) => unit.toNanos(value).toDouble / 1e9 // convert to seconds as double
      case None => value
    }
    otelHistogram.foreach(_.record(valueInSeconds, withAttributes(additionalAttributes)))

    // report original value because instrument is already configured with time unit
    if (additionalAttributes.nonEmpty) {
      // Kamon withTags creates a new instrument each time, so only do this if there are additional attributes
      kamonHistogram.foreach(_.withTags(TagSet.from(additionalAttributes)).record(value))
    } else {
      kamonHistogram.foreach(_.record(value))
    }
  }
}

private class FilodbMetrics(filodbMetricsConfig: Config) extends StrictLogging {

  private val otelEnabled = filodbMetricsConfig.as[Boolean]("otel-enabled")
  private val kamonEnabled = filodbMetricsConfig.as[Boolean]("kamon-enabled")
  private val closeables = scala.collection.mutable.ListBuffer.empty[AutoCloseable]

  // TODO for some reason, enabling this line fails DownsamplerMainSpec
  // Kamon.init() // intialize anyway until tracing is migrated to otel as well
  private val openTelemetry: OpenTelemetry = if (otelEnabled) {
    initializeOpenTelemetry()
  } else {
    OpenTelemetry.noop()
  }

  private val instrumentCache = new ConcurrentHashMap[String, MetricsInstrument]()

  lazy private val meter: Meter = openTelemetry.getMeter("filodb")

  // scalastyle:off method.length
  private def initializeOpenTelemetry(): OpenTelemetry = {

    val otelConfig: OTelMetricsConfig = OTelMetricsConfig.fromConfig(filodbMetricsConfig.getConfig("otel"))

    // Create resource with configured attributes
    val resourceBuilder = Resource.getDefault().toBuilder
    // TODO use standard resource attribute detectors so semconv is automatically followed
    otelConfig.resourceAttributes.foreach { case (key, value) =>
      resourceBuilder.put(AttributeKey.stringKey(key), value)
    }
    val resource = resourceBuilder.build()

    // Create exporter based on configuration
    val metricExporter: MetricExporter = otelConfig.exporterType.toLowerCase match {
      case "log" =>
        logger.info("Using log-based metrics exporter")
        OtlpJsonLoggingMetricExporter.create(AggregationTemporality.DELTA)
      case "otlp" =>
        logger.info(s"Using OTLP metrics exporter with endpoint: ${otelConfig.otlpEndpoint}")
        val b = OtlpGrpcMetricExporter.builder()
          .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
          .setEndpoint(otelConfig.otlpEndpoint.get)

        if (otelConfig.otlpTrustedCertsPath.isDefined) {
          b.setTrustedCertificates(Files.readAllBytes(Paths.get(otelConfig.otlpTrustedCertsPath.get)))
        }
        if (otelConfig.otlpClientKeyPath.isDefined && otelConfig.otlpClientCertPath.isDefined) {
          b.setClientTls(Files.readAllBytes(Paths.get(otelConfig.otlpClientKeyPath.get)),
                         Files.readAllBytes(Paths.get(otelConfig.otlpClientCertPath.get)))
        }
        otelConfig.otlpHeaders.foreach { case (key, value) =>
          b.addHeader(key, value)
        }
        b.build()
      case _ => throw new IllegalArgumentException(s"Unknown exporter type: ${otelConfig.exporterType}")
    }

    // Create periodic metric reader with delta aggregation
    val metricReader = PeriodicMetricReader.builder(metricExporter)
      .setInterval(Duration.ofSeconds(otelConfig.exportIntervalSeconds))
      .build()

    // Build SDK
    val sdkMeterProviderBuilder = SdkMeterProvider.builder()
      .setResource(resource)
      .registerMetricReader(metricReader)
    if (otelConfig.exponentialHistogram) {
      sdkMeterProviderBuilder.registerView(InstrumentSelector.builder().setType(InstrumentType.HISTOGRAM).build(),
        View.builder().setAggregation(Aggregation.base2ExponentialBucketHistogram(64, 20)).build())
    } else {
      import collection.JavaConverters._
      val timeBuckets = otelConfig.customHistogramBucketsTime.map(Double.box).asJava
      sdkMeterProviderBuilder.registerView(InstrumentSelector.builder()
        .setType(InstrumentType.HISTOGRAM).setUnit("seconds")
        .build(),
        View.builder().setAggregation(Aggregation.explicitBucketHistogram(timeBuckets)).build())
      val buckets = otelConfig.customHistogramBuckets.map(Double.box).asJava
      sdkMeterProviderBuilder.registerView(InstrumentSelector.builder()
        .setType(InstrumentType.HISTOGRAM)
        .build(),
        View.builder().setAggregation(Aggregation.explicitBucketHistogram(buckets)).build())
    }

    val sdk = OpenTelemetrySdk.builder()
      .setMeterProvider(sdkMeterProviderBuilder.build())
      .build()
    import scala.collection.JavaConverters._
    closeables ++= Classes.registerObservers(sdk).asScala
    closeables ++= Cpu.registerObservers(sdk).asScala
    closeables ++= MemoryPools.registerObservers(sdk).asScala
    closeables ++= Threads.registerObservers(sdk).asScala
    closeables ++= GarbageCollector.registerObservers(sdk, true).asScala
    closeables ++= SystemMetrics.registerObservers(sdk).asScala
    sdk
  }

  /**
   * Creates or retrieves a counter instrument with the given base attributes
   *
   * @param name instrument name
   * @param isBytes if true, appends "_bytes" to the name if not already present
   * @param baseAttributes base key-value pairs that will be included in all measurements
   * @return OTelCounter wrapper
   */
  def counter(name: String,
              isBytes: Boolean,
              timeUnit: Option[TimeUnit],
              baseAttributes: Map[String, String]): MetricsCounter = {
    val cacheKey = s"counter:$name"
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelCounter = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes, isCounter = true, timeUnit)
        Some(meter.counterBuilder(n).build())
      }

      val kamonCounter = if (!kamonEnabled) None else {
        // dont normalize name for kamon since it happens at publish time today
        if (isBytes)
          Some(Kamon.counter(name, MeasurementUnit.information.bytes).withTags(TagSet.from(baseAttributes)))
        else
          Some(Kamon.counter(name).withTags(TagSet.from(baseAttributes)))
      }

      val attributes = createAttributes(baseAttributes)
      MetricsCounter(otelCounter, kamonCounter, attributes)
    }).asInstanceOf[MetricsCounter]
  }

  /**
   * Creates or retrieves an up down counter instrument with the given base attributes
   *
   * @param name instrument name
   * @param isBytes if true, appends "_bytes" to the name if not already present
   * @param baseAttributes base key-value pairs that will be included in all measurements
   * @return OTelCounter wrapper
   */
  def upDownCounter(name: String,
                    isBytes: Boolean,
                    baseAttributes: Map[String, String]): MetricsUpDownCounter = {
    val cacheKey = s"upDownCounter:$name"
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelCounter = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes, isCounter = false, None) // upDownCounter is gauge behind the scenes
        Some(meter.upDownCounterBuilder(n).build())
      }

      val kamonCounter = if (!kamonEnabled) None else {
        // dont normalize name for kamon since it happens at publish time today
        if (isBytes)
          Some(Kamon.gauge(name, MeasurementUnit.information.bytes).withTags(TagSet.from(baseAttributes)))
        else
          Some(Kamon.gauge(name).withTags(TagSet.from(baseAttributes)))
      }

      val attributes = createAttributes(baseAttributes)
      MetricsUpDownCounter(otelCounter, kamonCounter, attributes)
    }).asInstanceOf[MetricsUpDownCounter]
  }

  /**
   * Creates or retrieves a gauge instrument with the given base attributes
   *
   * @param name instrument name
   * @param isBytes if true, appends "_bytes" to the name if not already present
   * @param baseAttributes base key-value pairs that will be included in all measurements
   * @return OTelGauge wrapper
   */
  def gauge(name: String,
            isBytes: Boolean,
            timeUnit: Option[TimeUnit],
            baseAttributes: Map[String, String]): MetricsGauge = {
    val cacheKey = s"gauge:$name"
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelGauge = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes, isCounter = true, timeUnit)
        Some(meter.gaugeBuilder(n).build())
      }

      val kamonGauge = if (!kamonEnabled) None else {
        // dont normalize name for kamon since it happens at publish time today
        if (isBytes)
          Some(Kamon.gauge(name, MeasurementUnit.information.bytes).withTags(TagSet.from(baseAttributes)))
        else
          Some(Kamon.gauge(name).withTags(TagSet.from(baseAttributes)))
      }

      val attributes = createAttributes(baseAttributes)
      MetricsGauge(otelGauge, kamonGauge, attributes)
    }).asInstanceOf[MetricsGauge]
  }

  /**
   * Creates or retrieves a histogram instrument with the given base attributes
   *
   * @param name instrument name
   * @param timeUnit if defined, appends "_seconds" to the name if not already present and
   *                 converts recorded values to seconds
   * @param baseAttributes base key-value pairs that will be included in all measurements
   * @return OTelHistogram wrapper
   */
  def histogram(name: String,
                timeUnit: Option[TimeUnit],
                baseAttributes: Map[String, String]): MetricsHistogram = {
    val cacheKey = s"histogram:$name"
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelHistogram = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes = false, isCounter = false, timeUnit)
        val hb = meter.histogramBuilder(n)
        if (timeUnit.isDefined) hb.setUnit("seconds")
        Some(hb.build())
      }

      val kamonHistogram = if (!kamonEnabled) None else {
        val mu = timeUnit.map {
          case TimeUnit.MILLISECONDS => MeasurementUnit.time.milliseconds
          case TimeUnit.SECONDS => MeasurementUnit.time.seconds
          case TimeUnit.NANOSECONDS => MeasurementUnit.time.nanoseconds
          case TimeUnit.MICROSECONDS => MeasurementUnit.time.microseconds
          case _ => throw new IllegalArgumentException(s"Unsupported time unit: $timeUnit")
        }
        // dont normalize name for kamon since it happens at publish time today
        val k = mu match {
          case Some(unit) => Kamon.histogram(name, unit).withTags(TagSet.from(baseAttributes))
          case None => Kamon.histogram(name).withTags(TagSet.from(baseAttributes))
        }
        Some(k)
      }

      val attributes = createAttributes(baseAttributes)
      MetricsHistogram(otelHistogram, kamonHistogram, timeUnit, attributes)
    }).asInstanceOf[MetricsHistogram]
  }

  private def normalizeMetricName(name: String,
                                  isBytes: Boolean,
                                  isCounter: Boolean,
                                  timeUnit: Option[TimeUnit]): String = {
    def validNameChar(char: Char): Char = if (char.isLetterOrDigit || char == '_' || char == ':') char else '_'
    val nameWithValidChars = name.map(validNameChar)
    val nameWithUnit = if (timeUnit.isDefined && !nameWithValidChars.endsWith("_seconds")) {
      nameWithValidChars + "_seconds"
    } else if (isBytes && !nameWithValidChars.endsWith("_bytes")) {
      nameWithValidChars + "_bytes"
    } else nameWithValidChars
    if (isCounter && !nameWithUnit.endsWith("_total") && !nameWithValidChars.endsWith("_total")) {
      nameWithUnit + "_total"
    } else nameWithUnit
  }

  private def createAttributes(attributeMap: Map[String, String]): Attributes = {
    val builder = Attributes.builder()
    attributeMap.foreach { case (key, value) =>
      builder.put(AttributeKey.stringKey(key), value)
    }
    builder.build()
  }

  /**
   * Shutdown the metrics system gracefully
   */
  def shutdown(): Unit = {
    logger.info("Metrics shutdown completed")
    openTelemetry match {
      case sdk: OpenTelemetrySdk =>
        sdk.shutdown()
      case _ =>
    }
    closeables.foreach(_.close())
    if (kamonEnabled) {
      Kamon.stop()
    }
  }
}

/**
 * Companion object providing singleton access. Example usage:
 * {{{
 *   FilodbMetrics.counter("http_requests_total", Map("dataset" -> "raw"))
 * }}}
 */
object FilodbMetrics {
  private lazy val instance: FilodbMetrics = {
    val config = GlobalConfig.systemConfig.getConfig("filodb.metrics")
    new FilodbMetrics(config)
  }

  def bytesGauge(name: String,
                 baseAttributes: Map[String, String] = Map.empty): MetricsGauge = {
    instance.gauge(name, isBytes = true, None, baseAttributes)
  }

  def gauge(name: String,
            baseAttributes: Map[String, String] = Map.empty): MetricsGauge = {
    instance.gauge(name, isBytes = false, None, baseAttributes)
  }

  def timeGauge(name: String,
                timeUnit: TimeUnit,
                baseAttributes: Map[String, String] = Map.empty): MetricsGauge = {
    instance.gauge(name, isBytes = false, Some(timeUnit), baseAttributes)
  }

  def bytesCounter(name: String,
                   baseAttributes: Map[String, String] = Map.empty): MetricsCounter = {
    instance.counter(name, isBytes = true, None, baseAttributes)
  }

  def counter(name: String,
              baseAttributes: Map[String, String] = Map.empty): MetricsCounter = {
    instance.counter(name, isBytes = false, None, baseAttributes)
  }

  def timeCounter(name: String,
                  timeUnit: TimeUnit,
                  baseAttributes: Map[String, String] = Map.empty): MetricsCounter = {
    instance.counter(name, isBytes = false, Some(timeUnit), baseAttributes)
  }

  def bytesUpDownCounter(name: String,
                         baseAttributes: Map[String, String] = Map.empty): MetricsUpDownCounter = {
    instance.upDownCounter(name, isBytes = true, baseAttributes)
  }

  def upDownCounter(name: String,
                    baseAttributes: Map[String, String]): MetricsUpDownCounter = {
    instance.upDownCounter(name, isBytes = false, baseAttributes)
  }

  def histogram(name: String,
                baseAttributes: Map[String, String] = Map.empty): MetricsHistogram = {
    instance.histogram(name, None, baseAttributes)
  }

  def timeHistogram(name: String,
                    timeUnit: TimeUnit,
                    baseAttributes: Map[String, String] = Map.empty): MetricsHistogram = {
    instance.histogram(name, Some(timeUnit), baseAttributes)
  }

  def shutdown(): Unit = {
    instance.shutdown()
  }
}