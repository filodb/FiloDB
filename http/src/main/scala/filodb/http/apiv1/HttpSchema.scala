package filodb.http.apiv1

// Prometheus API compatible response envelopes
sealed trait HttpSuccess
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
}

final case class HttpShardState(shard: Int, status: String, address: String)