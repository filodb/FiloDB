package filodb.http.promcompat

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets

import org.xerial.snappy.Snappy
import remote.RemoteStorage.WriteRequest

/** HTTP status code plus response body from a remote-write POST. */
final case class WriteResult(code: Int, body: String)

object PromRemoteClient {
  def encode(req: WriteRequest): Array[Byte] = Snappy.compress(req.toByteArray)

  private def enc(q: String): String = URLEncoder.encode(q, "UTF-8")

  def instantUrl(base: String, q: String, t: Long): String =
    s"$base/api/v1/query?query=${enc(q)}&time=$t"

  def rangeUrl(base: String, q: String, s: Long, e: Long, step: Int): String =
    s"$base/api/v1/query_range?query=${enc(q)}&start=$s&end=$e&step=$step"

  private def readBody(stream: InputStream): String =
    if (stream == null) ""
    else {
      val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
      try {
        val sb = new StringBuilder
        var line = reader.readLine()
        while (line != null) { sb.append(line); line = reader.readLine() }
        sb.toString
      } finally reader.close()
    }
}

class PromRemoteClient(baseUrl: String) {
  import PromRemoteClient._

  def remoteWrite(req: WriteRequest): WriteResult = {
    val conn = new URL(s"$baseUrl/api/v1/write").openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("POST")
      conn.setDoOutput(true)
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(30000)
      conn.setRequestProperty("Content-Encoding", "snappy")
      conn.setRequestProperty("Content-Type", "application/x-protobuf")
      conn.setRequestProperty("X-Prometheus-Remote-Write-Version", "0.1.0")
      val body = encode(req)
      val os = conn.getOutputStream
      try os.write(body) finally os.close()
      val code = conn.getResponseCode
      // Prometheus returns the failure reason (e.g. "out of order sample") in the body on non-2xx.
      val respBody = readBody(if (code >= 400) conn.getErrorStream else conn.getInputStream)
      WriteResult(code, respBody)
    } finally {
      conn.disconnect()
    }
  }

  private def get(url: String): String = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("GET")
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(60000)
      val code = conn.getResponseCode
      readBody(if (code >= 400) conn.getErrorStream else conn.getInputStream)
    } finally {
      conn.disconnect()
    }
  }

  def instantQuery(promql: String, timeSec: Long): String = get(instantUrl(baseUrl, promql, timeSec))
  def rangeQuery(promql: String, startSec: Long, endSec: Long, stepSec: Int): String =
    get(rangeUrl(baseUrl, promql, startSec, endSec, stepSec))
}
