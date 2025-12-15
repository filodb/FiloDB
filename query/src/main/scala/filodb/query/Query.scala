package filodb.query

import com.typesafe.scalalogging.{Logger, StrictLogging}

import filodb.core.metrics.FilodbMetrics

/**
  * ExecPlan objects cannot have loggers as vals because they would then not be serializable.
  * All Query Engine constructs should use this logger and provide more details of the construct
  * in the log message.
  */
object Query extends StrictLogging {
  val qLogger: Logger = logger
  // TODO refine with dataset tag
  protected[query] val droppedSamples = FilodbMetrics.counter("query-dropped-samples")
  val timeOutCounter = FilodbMetrics.counter("filodb-ask-timeout")
}
