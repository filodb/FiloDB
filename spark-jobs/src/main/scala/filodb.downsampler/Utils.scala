package filodb.downsampler

import java.text.SimpleDateFormat
import java.util.TimeZone

object Utils {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
  def millisToString(millis: Long): String = sdf.format(millis)

}
