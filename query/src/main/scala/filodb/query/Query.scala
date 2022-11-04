package filodb.query

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon

/**
  * ExecPlan objects cannot have loggers as vals because they would then not be serializable.
  * All Query Engine constructs should use this logger and provide more details of the construct
  * in the log message.
  */
object Query extends StrictLogging {
  protected[query] val qLogger: Logger = logger
  // TODO refine with dataset tag
  protected[query] val droppedSamples = Kamon.counter("query-dropped-samples").withoutTags
}
