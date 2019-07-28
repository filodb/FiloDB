package filodb.query

trait TsdbQueryParams

case class PromQlQueryParams(promEndPoint : String, promQl: String, start: Long, step: Long, end: Long )
  extends TsdbQueryParams

object DummyPromQlQueryParams extends PromQlQueryParams ("", "", 0, 0, 0) {
}