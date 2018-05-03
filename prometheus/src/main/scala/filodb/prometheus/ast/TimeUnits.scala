package filodb.prometheus.ast


trait TimeUnits {

  /**
    * Time durations are specified as a number
    * followed immediately by one of the following units:
    * s - seconds
    * m - minutes
    * h - hours
    * d - days
    * w - weeks
    * y - years
    */

  sealed trait TimeUnit {
    def millis: Long
  }

  case object Second extends TimeUnit {
    override def millis: Long = 1000L
  }

  case object Minute extends TimeUnit {
    override def millis: Long = Second.millis * 60
  }

  case object Hour extends TimeUnit {
    override def millis: Long = Minute.millis * 60
  }

  case object Day extends TimeUnit {
    override def millis: Long = Hour.millis * 24
  }

  case object Week extends TimeUnit {
    override def millis: Long = Day.millis * 7
  }

  case object Year extends TimeUnit {
    override def millis: Long = Week.millis * 52
  }

  case class Duration(scale: Int, timeUnit: TimeUnit) {
    if (scale <= 0) throw new IllegalArgumentException("Duration should be greater than zero")
    val millis = scale * timeUnit.millis
  }

  case class Offset(duration: Duration)

}
