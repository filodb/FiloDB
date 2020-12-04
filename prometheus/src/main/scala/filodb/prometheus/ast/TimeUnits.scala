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
    * i - factor of step (aka interval)
    * */

  sealed trait TimeUnit {
    def millis(step: Long): Long
  }

  case object Second extends TimeUnit {
    override def millis(step: Long): Long = 1000L
  }

  case object Minute extends TimeUnit {
    override def millis(step: Long): Long = Second.millis(step) * 60
  }

  case object Hour extends TimeUnit {
    override def millis(step: Long): Long = Minute.millis(step) * 60
  }

  case object Day extends TimeUnit {
    override def millis(step: Long): Long = Hour.millis(step) * 24
  }

  case object Week extends TimeUnit {
    override def millis(step: Long): Long = Day.millis(step) * 7
  }

  case object Year extends TimeUnit {
    override def millis(step: Long): Long = Week.millis(step) * 52
  }

  case object IntervalMultiple extends TimeUnit {
    override def millis(step: Long): Long = {
      require(step > 0, "Interval multiple notation was used in lookback/range without valid step")
      step
    }
  }

  case class Duration(scale: Double, timeUnit: TimeUnit) {
    if (scale <= 0) throw new IllegalArgumentException("Duration should be greater than zero")
    def millis(step: Long): Long = (scale * timeUnit.millis(step)).toLong
  }

  case class Offset(duration: Duration)

  sealed trait RangeProducer {
    def interval: Duration
  }

  case class TimeInterval(interval: Duration) extends RangeProducer {
  }

  case class Subquery(interval: Duration, step: Option[Duration]) extends RangeProducer {
  }


}
