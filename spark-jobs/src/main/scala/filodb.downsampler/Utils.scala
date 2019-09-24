package filodb.downsampler

import java.text.SimpleDateFormat
import java.util.TimeZone

object Utils {
  private val sdf = new SimpleDateFormat
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
  def millisToString(millis: Long): String = sdf.format(millis)

}
