package filodb.query

trait TsdbQueryParams

case class PromQlQueryParams(promEndPoint : String, promQl: String, start: Long, step: Long, end: Long, spread: Int,
                             limit: Int, processFailure : Boolean= true ) extends TsdbQueryParams

object DummyPromQlQueryParams extends PromQlQueryParams ("", "", 0, 0,
  0, 1, 0)