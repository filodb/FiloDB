package filodb.query

import scala.concurrent.duration._

case class PromQlInvocationParams(endpoint: String, promQl: String, start: Long, step: Long, end: Long,
                                  readTimeout: Duration = 60.seconds, spread: Option[Int] = None,
                                  processFailure: Boolean = true)

