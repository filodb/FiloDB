package filodb.core.metrics

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes, AttributesBuilder}
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
import monix.execution.Scheduler
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import oshi.SystemInfo

import filodb.core.GlobalConfig

/**
 * Configuration for OpenTelemetry metrics
 */
case class OTelMetricsConfig(exportIntervalSeconds: Int = 60,
                             resourceAttributes: Map[String, String],
                             otlpHeaders: Map[String, String],
                             exporterFactoryClassName: String, // Fully qualified class name of MetricExporterFactory
                             exponentialHistogram: Boolean,
                             customHistogramBucketsTime: List[Double], // used only if exponentialHistogram=false
                             customHistogramBuckets: List[Double], // used only if exponentialHistogram=false
                             otlpEndpoint: Option[String],
                             otlpTrustedCertsPath: Option[String],
                             // one of client cert/key or p12 keystore must be provided for mTLS
                             otlpClientCertPath: Option[String],
                             otlpClientKeyPath: Option[String],
                             otlpClientP12KeystorePath: Option[String])

/**
 * Factory interface for creating MetricExporter instances
 */
trait MetricExporterFactory {
  /**
   * Creates a MetricExporter instance
   * @param config The OTelMetricsConfig to use for configuration
   * @return A configured MetricExporter
   */
  def create(config: OTelMetricsConfig): MetricExporter
}

/**
 * Factory for creating OtlpGrpcMetricExporter instances
 */
class OtlpGrpcMetricExporterFactory extends MetricExporterFactory {
  override def create(config: OTelMetricsConfig): MetricExporter = {
    val b = OtlpGrpcMetricExporter.builder()
      .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
      .setEndpoint(config.otlpEndpoint.getOrElse(
        throw new IllegalArgumentException("otlp-endpoint must be configured when using OTLP exporter")))

    if (config.otlpTrustedCertsPath.isDefined &&
        config.otlpClientKeyPath.isDefined &&
        config.otlpClientCertPath.isDefined) {
      b.setTrustedCertificates(Files.readAllBytes(Paths.get(config.otlpTrustedCertsPath.get)))
      b.setClientTls(Files.readAllBytes(Paths.get(config.otlpClientKeyPath.get)),
        Files.readAllBytes(Paths.get(config.otlpClientCertPath.get)))
    } else if (config.otlpTrustedCertsPath.isDefined && config.otlpClientP12KeystorePath.isDefined) {
      // TODO support later
    }
    config.otlpHeaders.foreach { case (key, value) =>
      b.addHeader(key, value)
    }
    b.build()
  }
}

/**
 * Factory for creating OtlpJsonLoggingMetricExporter instances
 */
class LogMetricExporterFactory extends MetricExporterFactory {
  override def create(config: OTelMetricsConfig): MetricExporter = {
    OtlpJsonLoggingMetricExporter.create(AggregationTemporality.DELTA)
  }
}

object OTelMetricsConfig {

  /**
   * Creates a MetricExporterFactory instance using reflection
   * @param className Fully qualified class name of the factory
   * @return Instance of MetricExporterFactory
   */
  def instantiateExporterFactory(className: String): MetricExporterFactory = {
    try {
      val clazz = Class.forName(className)
      clazz.getDeclaredConstructor().newInstance().asInstanceOf[MetricExporterFactory]
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Factory class not found: $className", e)
      case e: ClassCastException =>
        throw new IllegalArgumentException(
          s"Class $className does not implement MetricExporterFactory trait", e)
      case e: Exception =>
        throw new IllegalArgumentException(s"Failed to instantiate factory: $className", e)
    }
  }

  def fromConfig(metricsConfig: Config): OTelMetricsConfig = {
    OTelMetricsConfig(
      otlpEndpoint = metricsConfig.as[Option[String]]("otlp-endpoint"),
      exportIntervalSeconds = metricsConfig.as[Int]("export-interval-seconds"),
      resourceAttributes = metricsConfig.as[Map[String, String]]("resource-attributes"),
      exporterFactoryClassName = metricsConfig.as[String]("exporter-factory-class-name"),
      exponentialHistogram = metricsConfig.as[Boolean]("exponential-histogram"),
      customHistogramBucketsTime = metricsConfig.as[List[Double]]("custom-histogram-buckets-time").sorted,
      customHistogramBuckets = metricsConfig.as[List[Double]]("custom-histogram-buckets").sorted,
      // otlp specific settings
      otlpHeaders = metricsConfig.as[Map[String, String]]("otlp-headers"),
      otlpTrustedCertsPath = metricsConfig.as[Option[String]]("otlp-trusted-certs-path"),
      otlpClientCertPath = metricsConfig.as[Option[String]]("otlp-client-cert-path"),
      otlpClientKeyPath = metricsConfig.as[Option[String]]("otlp-client-key-path"),
      otlpClientP12KeystorePath = metricsConfig.as[Option[String]]("otlp-client-p12-keystore-path")
      // TODO add support for passwords if needed later
    )
  }
}

/**
 * Wrapper for OpenTelemetry instruments with additional key-value pairs
 */
sealed trait MetricsInstrument {
  def baseAttributesBuilder: AttributesBuilder
  def withAttributes(additionalAttributes: Map[String, String]): Attributes = {
    additionalAttributes.foreach { case (key, value) =>
      baseAttributesBuilder.put(AttributeKey.stringKey(key), value)
    }
    baseAttributesBuilder.build()
  }
}

case class MetricsCounter(otelCounter: Option[DoubleCounter],
                          kamonCounter: Option[kamon.metric.Counter],
                          timeUnit: Option[TimeUnit],
                          baseAttributesBuilder: AttributesBuilder) extends MetricsInstrument {
  def increment(value: Long = 1, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    val valueInSeconds = timeUnit match {
      case Some(unit) => unit.toNanos(value).toDouble / 1e9 // convert to seconds as double
      case None => value
    }
    otelCounter.foreach(_.add(valueInSeconds, withAttributes(additionalAttributes)))
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
                                baseAttributesBuilder: AttributesBuilder) extends MetricsInstrument {
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
                        timeUnit: Option[TimeUnit],
                        baseAttributesBuilder: AttributesBuilder) extends MetricsInstrument {
  def update(value: Double, additionalAttributes: Map[String, String] = Map.empty): Unit = {
    val valueInSeconds = timeUnit match {
      case Some(unit) => unit.toNanos(value.toLong).toDouble / 1e9 // convert to seconds as double
      case None => value
    }
    otelGauge.foreach(_.set(valueInSeconds, withAttributes(additionalAttributes)))
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
                            baseAttributesBuilder: AttributesBuilder) extends MetricsInstrument {
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

  val otelConfig: OTelMetricsConfig = OTelMetricsConfig.fromConfig(filodbMetricsConfig.getConfig("otel"))

  // TODO for some reason, enabling this line fails DownsamplerMainSpec
  // Kamon.init() // intialize anyway until tracing is migrated to otel as well
  private val openTelemetry: OpenTelemetry = if (otelEnabled) {
    initializeOpenTelemetry()
  } else {
    OpenTelemetry.noop()
  }

  private val instrumentCache = new ConcurrentHashMap[(String, Map[String, String]), MetricsInstrument]()

  lazy private val meter: Meter = openTelemetry.getMeter("filodb")

  // scalastyle:off method.length
  private def initializeOpenTelemetry(): OpenTelemetry = {

    // Create resource with configured attributes
    val resourceBuilder = Resource.getDefault().toBuilder
    // TODO use standard resource attribute detectors so semconv is automatically followed
    otelConfig.resourceAttributes.foreach { case (key, value) =>
      resourceBuilder.put(AttributeKey.stringKey(key), value)
    }
    val resource = resourceBuilder.build()

    // Create exporter using the configured factory
    logger.info(s"Using ${otelConfig.exporterFactoryClassName} metrics exporter")
    val metricExporter: MetricExporter =
        OTelMetricsConfig.instantiateExporterFactory(otelConfig.exporterFactoryClassName).create(otelConfig)

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
        .setType(InstrumentType.HISTOGRAM).setUnit("counts")
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
    closeables ++= registerProcessMetrics(sdk)
    sdk
  }

  private def registerProcessMetrics(sdk: OpenTelemetrySdk) = {
    val systemInfo = new SystemInfo
    val osInfo = systemInfo.getOperatingSystem
    val processInfo = osInfo.getProcess(osInfo.getProcessId)

    implicit val sch: Scheduler = monix.execution.Scheduler.global
    val observables = new ListBuffer[AutoCloseable]()
    val strMem = "process_memory_bytes"
    val strCpu = "process_cpu_time_total_seconds"
    val strMajorFaults = "process_major_page_faults_total"
    val strMinorFaults = "process_minor_page_faults_total"
    val strContextSwitches = "process_context_switches_total"

    if (kamonEnabled) {
      val memRss = Kamon.gauge(strMem).withTag("type", "rss")
      val memVms = Kamon.gauge(strMem).withTag("type", "vms")
      val cpuUserTime = Kamon.gauge(strMem, MeasurementUnit.time.milliseconds).withTag("type", "user")
      val cpuSystemTime = Kamon.gauge(strMem, MeasurementUnit.time.milliseconds).withTag("type", "system")
      val majorPageFaults = Kamon.gauge(strMajorFaults).withoutTags()
      val minorPageFaults = Kamon.gauge(strMinorFaults).withoutTags()
      val contextSwitches = Kamon.gauge(strContextSwitches).withoutTags()

      val future = Observable.intervalWithFixedDelay(0.seconds, otelConfig.exportIntervalSeconds.seconds).foreach { _ =>
        processInfo.updateAttributes()
        memRss.update(processInfo.getResidentSetSize)
        memVms.update(processInfo.getVirtualSize)
        cpuSystemTime.update(processInfo.getKernelTime.toDouble)
        cpuUserTime.update(processInfo.getUserTime.toDouble)
        majorPageFaults.update(processInfo.getMajorFaults.toDouble)
        minorPageFaults.update(processInfo.getMinorFaults.toDouble)
        contextSwitches.update(processInfo.getContextSwitches.toDouble)
      }
      observables.append(() => future.cancel())
    }

    if (otelEnabled) {
      val meter = sdk.getMeter("filodb-process-metrics")
      observables.append(meter.gaugeBuilder(strMem).ofLongs().buildWithCallback((r: ObservableLongMeasurement) => {
          processInfo.updateAttributes()
          r.record(processInfo.getResidentSetSize, Attributes.builder().put("type", "rss").build())
          r.record(processInfo.getVirtualSize, Attributes.builder().put("type", "vms").build())
        }))
      observables.append(meter.counterBuilder(strCpu).ofDoubles()
        .buildWithCallback((r: ObservableDoubleMeasurement) => {
          processInfo.updateAttributes()
          // convert ms to seconds
          r.record(processInfo.getUserTime.toDouble / 1000, Attributes.builder().put("type", "user").build())
          r.record(processInfo.getKernelTime.toDouble / 1000, Attributes.builder().put("type", "system").build())
        }))
      observables.append(meter.counterBuilder(strMajorFaults).buildWithCallback((r: ObservableLongMeasurement) => {
          processInfo.updateAttributes()
          r.record(processInfo.getMajorFaults)
        }))
      observables.append(meter.counterBuilder(strMinorFaults).buildWithCallback((r: ObservableLongMeasurement) => {
          processInfo.updateAttributes()
          r.record(processInfo.getMinorFaults)
        }))
      observables.append(meter.counterBuilder(strContextSwitches).buildWithCallback((r: ObservableLongMeasurement) => {
          processInfo.updateAttributes()
          r.record(processInfo.getContextSwitches)
        }))
    }
    observables
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
    require(!isBytes || timeUnit.isEmpty, "isBytes and timeUnit cannot both be set")
    val cacheKey = (s"counter:$name", baseAttributes)
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelCounter = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes, isCounter = true, timeUnit)
        Some(meter.counterBuilder(n).ofDoubles().build())
      }

      val kamonCounter = if (!kamonEnabled) None else {
        // dont normalize name for kamon since it happens at publish time today
        if (isBytes)
          Some(Kamon.counter(name, MeasurementUnit.information.bytes).withTags(TagSet.from(baseAttributes)))
        else {
          val mu = timeUnit.map(toKamonTimeUnit)
          val k = mu match {
            case Some(unit) => Kamon.counter(name, unit).withTags(TagSet.from(baseAttributes))
            case None => Kamon.counter(name).withTags(TagSet.from(baseAttributes))
          }
          Some(k)
        }
      }

      val attributes = createAttributesBuilder(baseAttributes)
      MetricsCounter(otelCounter, kamonCounter, timeUnit, attributes)
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
    val cacheKey = (s"upDownCounter:$name", baseAttributes)
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

      val attributes = createAttributesBuilder(baseAttributes)
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
    require(!isBytes || timeUnit.isEmpty, "isBytes and timeUnit cannot both be set")
    val cacheKey = (s"gauge:$name", baseAttributes)
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelGauge = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes, isCounter = false, timeUnit)
        Some(meter.gaugeBuilder(n).build())
      }

      val kamonGauge = if (!kamonEnabled) None else {
        // dont normalize name for kamon since it happens at publish time today
        if (isBytes)
          Some(Kamon.gauge(name, MeasurementUnit.information.bytes).withTags(TagSet.from(baseAttributes)))
        else {
          val mu = timeUnit.map(toKamonTimeUnit)
          val k = mu match {
            case Some(unit) => Kamon.gauge(name, unit).withTags(TagSet.from(baseAttributes))
            case None => Kamon.gauge(name).withTags(TagSet.from(baseAttributes))
          }
          Some(k)
        }
      }

      val attributes = createAttributesBuilder(baseAttributes)
      MetricsGauge(otelGauge, kamonGauge, timeUnit, attributes)
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
    val cacheKey = (s"histogram:$name", baseAttributes)
    instrumentCache.getOrElseUpdate(cacheKey, { _ =>

      val otelHistogram = if (!otelEnabled) None else {
        val n = normalizeMetricName(name, isBytes = false, isCounter = false, timeUnit)
        val hb = meter.histogramBuilder(n)
        // we set unit as "counts" so that we can set the custom bucketing for the histogram correctly
        if (timeUnit.isDefined) hb.setUnit("seconds") else hb.setUnit("counts")
        Some(hb.build())
      }

      val kamonHistogram = if (!kamonEnabled) None else {
        val mu = timeUnit.map(toKamonTimeUnit)
        // dont normalize name for kamon since it happens at publish time today
        val k = mu match {
          case Some(unit) => Kamon.histogram(name, unit).withTags(TagSet.from(baseAttributes))
          case None => Kamon.histogram(name).withTags(TagSet.from(baseAttributes))
        }
        Some(k)
      }

      val attributes = createAttributesBuilder(baseAttributes)
      MetricsHistogram(otelHistogram, kamonHistogram, timeUnit, attributes)
    }).asInstanceOf[MetricsHistogram]
  }

  private def toKamonTimeUnit(timeUnit: TimeUnit): MeasurementUnit = timeUnit match {
    case TimeUnit.NANOSECONDS => MeasurementUnit.time.nanoseconds
    case TimeUnit.MICROSECONDS => MeasurementUnit.time.microseconds
    case TimeUnit.MILLISECONDS => MeasurementUnit.time.milliseconds
    case TimeUnit.SECONDS => MeasurementUnit.time.seconds
    case _ => throw new IllegalArgumentException(s"Unsupported time unit: $timeUnit")
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

  private def createAttributesBuilder(attributeMap: Map[String, String]): AttributesBuilder = {
    val builder = Attributes.builder()
    attributeMap.foreach { case (key, value) =>
      builder.put(AttributeKey.stringKey(key), value)
    }
    builder
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