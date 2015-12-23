package filodb.core.util

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

trait FiloLogging {
  // auto checks with macros for log level enabled.
  @transient lazy val flow = Logger(LoggerFactory.getLogger("filodb.flow"))

  @transient lazy val metrics= Logger(LoggerFactory.getLogger("filodb.metrics"))
}
