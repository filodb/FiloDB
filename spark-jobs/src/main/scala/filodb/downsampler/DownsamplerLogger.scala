package filodb.downsampler

import com.typesafe.scalalogging.{Logger, StrictLogging}

object DownsamplerLogger extends StrictLogging {
  lazy protected[downsampler] val dsLogger: Logger = logger
}
