package filodb.memory.format

import com.typesafe.scalalogging.{Logger, StrictLogging}

object MemoryLogger extends StrictLogging {
  protected[memory] val mLogger: Logger = logger

}
