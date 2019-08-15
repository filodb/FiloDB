package filodb.query

import com.typesafe.config.Config

case class PromQlInvocationParams(config: Config, promQl: String, start: Long, step: Long, end: Long,
                                  spread: Option[Int] = None, processFailure: Boolean = true)

