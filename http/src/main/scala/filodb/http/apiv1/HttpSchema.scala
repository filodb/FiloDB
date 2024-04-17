package filodb.http.apiv1

// Prometheus API compatible response envelopes
sealed trait HttpSuccess

/**
 * Schema for http liveness checks
 * @param Status
 */
final case class HttpLiveness(Status: String) extends HttpSuccess
final case class HttpList[A](status: String, data: Seq[A]) extends HttpSuccess
final case class HttpError(errorType: String,
                           error: String,
                           status: String = "error",
                           stackTrace: Seq[String] = Nil)

object HttpSchema {
  def httpList[A](data: Seq[A]): HttpList[A] = HttpList("success", data)
  def httpErr(errorType: String, errorString: String): HttpError = HttpError(errorType, errorString)
  def httpErr(exception: Exception): HttpError =
    HttpError(exception.getClass.getName, exception.getMessage,
              stackTrace = exception.getStackTrace.toSeq.map(_.toString))

  def httpLiveness(status: String): HttpLiveness = HttpLiveness(status)
}

final case class HttpShardState(shard: Int, status: String, address: String)
final case class HttpShardDetails(shard: Int, status: String)
final case class HttpShardStateByAddress(address: String, shardList: Seq[HttpShardDetails])