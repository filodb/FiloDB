package filodb.query

import com.typesafe.config.Config

case class PromQlInvocationParams(config: Config, promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long,
                                  spread: Option[Int] = None, processFailure: Boolean = true)

