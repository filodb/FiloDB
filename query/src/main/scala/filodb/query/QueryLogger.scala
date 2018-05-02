package filodb.query

import com.typesafe.scalalogging.{Logger, StrictLogging}

/**
  * ExecPlan objects cannot have loggers as vals because they would then not be serializable.
  * All Query Engine constructs should use this logger and provide more details of the construct
  * in the log message.
  */
object QueryLogger extends StrictLogging {
  protected[query] val qLogger: Logger = logger
}
