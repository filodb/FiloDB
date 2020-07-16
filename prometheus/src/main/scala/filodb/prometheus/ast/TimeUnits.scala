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
    def millis(step: Option[Long]): Long
  }

  case object Second extends TimeUnit {
    override def millis(step: Option[Long]): Long = 1000L
  }

  case object Minute extends TimeUnit {
    override def millis(step: Option[Long]): Long = Second.millis(step) * 60
  }

  case object Hour extends TimeUnit {
    override def millis(step: Option[Long]): Long = Minute.millis(step) * 60
  }

  case object Day extends TimeUnit {
    override def millis(step: Option[Long]): Long = Hour.millis(step) * 24
  }

  case object Week extends TimeUnit {
    override def millis(step: Option[Long]): Long = Day.millis(step) * 7
  }

  case object Year extends TimeUnit {
    override def millis(step: Option[Long]): Long = Week.millis(step) * 52
  }

  case object IntervalFactor extends TimeUnit {
    override def millis(step: Option[Long]): Long = {
      step.getOrElse(throw new IllegalArgumentException("Interval factor lookback/range notation was used " +
        "in expression, but step parameter was not provided in query"))
    }
  }

  case class Duration(scale: Int, timeUnit: TimeUnit) {
    if (scale <= 0) throw new IllegalArgumentException("Duration should be greater than zero")
    def millis(step: Option[Long]): Long = scale * timeUnit.millis(step)
  }

  case class Offset(duration: Duration)

}
