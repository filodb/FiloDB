package filodb.downsampler

import com.typesafe.scalalogging.{Logger, StrictLogging}

object DownsamplerLogger extends StrictLogging {
  protected[downsampler] val dsLogger: Logger = logger
}
