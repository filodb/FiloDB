package filodb.prometheus.ast

object SubqueryUtils {

  def getStartForFastSubquery(startMs: Long, stepMs: Long) : Long = {
    val remainder = startMs % stepMs
    if (remainder == 0) {
      startMs
    } else {
      startMs - remainder + stepMs
    }
  }

  def getEndForFastSubquery(endMs: Long, stepMs: Long): Long = {
    val remainder = endMs % stepMs
    if (remainder == 0) {
      endMs
    } else {
      endMs - remainder
    }
  }

  // According to PromQL docs, the subquery step is an optional parameter and
  // defaults to Prometheus global evaluation_interval configured in prometheus yml
  // Mosaic does not have a default step, defaulting to hard coded 60 seconds here.
  def getSubqueryStepMs(subqueryStep : Option[Duration]): Long = {
    var subqueryStepToUseMs = 60000L
    if (subqueryStep.isDefined) {
      subqueryStepToUseMs = subqueryStep.get.millis(1L)
    }
    subqueryStepToUseMs
  }
}
