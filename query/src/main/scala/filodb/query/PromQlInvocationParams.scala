package filodb.query

case class PromQlInvocationParams(endpoint: String, promQl: String, start: Long, step: Long, end: Long,
                             spread: Option[Int] = None, processFailure: Boolean = true)

