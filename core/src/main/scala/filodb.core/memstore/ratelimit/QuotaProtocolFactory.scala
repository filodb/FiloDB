package filodb.core.memstore.ratelimit

import scala.util.{Failure, Success}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.core.ConfigurableInstance

/**
 * Loads a `QuotaExceededProtocol` implementation from configuration via reflection so that the
 * `core` module never directly depends on Kafka (or any other transport-specific) jars. The
 * implementation class must be on the classpath of the FiloDB JVM and expose a public
 * `Config => QuotaExceededProtocol` companion factory or a `(Config)` constructor.
 *
 * Config block:
 * {{{
 * filodb.quota-protocol {
 *   enabled = false
 *   class = "filodb.kafka.KafkaQuotaProtocolPublisher"
 *   # ... impl-specific keys live alongside, the impl receives the whole sub-config
 * }
 * }}}
 *
 * When `enabled = false` (or the block is absent) we hand back `NoActionQuotaProtocol`, which is
 * the same default used by `CardinalityTracker`'s default arg.
 */
object QuotaProtocolFactory extends ConfigurableInstance {

  val ConfigPath = "quota-protocol"
  val EnabledKey = "enabled"
  val ClassKey = "class"

  /**
   * Build a `QuotaExceededProtocol` from the given **filodb** config (i.e. the sub-config
   * already scoped at `filodb.*`). Failures during load fall back to `NoActionQuotaProtocol`
   * with a logged error so a misconfigured publisher never takes ingestion down.
   */
  def fromConfig(filodbConfig: Config): QuotaExceededProtocol = {
    if (!filodbConfig.hasPath(ConfigPath)) return NoActionQuotaProtocol
    val sub = filodbConfig.getConfig(ConfigPath)
    val enabled = sub.hasPath(EnabledKey) && sub.getBoolean(EnabledKey)
    if (!enabled) return NoActionQuotaProtocol
    if (!sub.hasPath(ClassKey)) {
      logger.warn(s"$ConfigPath.$EnabledKey=true but $ConfigPath.$ClassKey is unset; " +
        s"falling back to NoActionQuotaProtocol")
      return NoActionQuotaProtocol
    }
    val fqcn = sub.getString(ClassKey)
    instantiate(fqcn, sub) match {
      case Success(p) =>
        logger.info(s"Loaded QuotaExceededProtocol implementation $fqcn")
        p
      case Failure(t) =>
        logger.error(s"Failed to load QuotaExceededProtocol implementation $fqcn; " +
          s"falling back to NoActionQuotaProtocol", t)
        NoActionQuotaProtocol
    }
  }

  // Tries `(Config)` constructor first, then no-arg constructor as a courtesy for very simple
  // impls / test stubs that don't need the config. The class must extend QuotaExceededProtocol.
  private def instantiate(fqcn: String, sub: Config): scala.util.Try[QuotaExceededProtocol] = {
    createClass[QuotaExceededProtocol](fqcn).flatMap { clazz =>
      createInstance[QuotaExceededProtocol](
        clazz, Seq(classOf[Config] -> sub.asInstanceOf[AnyRef]))
        .recoverWith { case _ => createInstance[QuotaExceededProtocol](clazz) }
    }
  }
}

/** Lifecycle hook so factory-loaded impls can release Kafka clients / threads on shutdown. */
trait CloseableQuotaProtocol extends QuotaExceededProtocol with AutoCloseable

object QuotaProtocolLogging extends StrictLogging {
  def logDropped(reason: String): Unit = logger.debug(s"Dropping quota breach event: $reason")
}
