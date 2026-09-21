package filodb.http.promcompat

final case class Tolerance(absolute: Double, percent: Double)
object Tolerance {
  def parse(s: String): Tolerance = {
    val t = if (s == null) "" else s.trim
    if (t.isEmpty) Tolerance(0.0, 0.0)
    else if (t.endsWith("%")) Tolerance(0.0, t.dropRight(1).trim.toDouble / 100.0)
    else Tolerance(t.toDouble, 0.0)
  }
}

final case class CompareOutcome(matched: Boolean, detail: String)

object ResultComparator {
  private val stripped = Set("__name__", "_metric_", "__shard__", "_step_")

  private def cleanLabels(m: Map[String, String]): Map[String, String] = m -- stripped

  private def valuesMatch(v1: Double, v2: Double, tol: Tolerance): Boolean = {
    if (v1 == v2) true
    else if (math.abs(v1 - v2) <= tol.absolute) true
    else if (tol.percent > 0.0 && v2 != 0.0 && math.abs(v1 - v2) / math.abs(v2) < tol.percent) true
    else false
  }

  private def pointsMatch(a: Seq[PromPoint], b: Seq[PromPoint], tol: Tolerance): Option[String] = {
    val am = a.map(p => p.tsSec -> p.value).toMap
    val bm = b.map(p => p.tsSec -> p.value).toMap
    def isNaNAt(m: Map[Long, Double], ts: Long): Boolean = m.get(ts).exists(_.isNaN)
    // A timestamp is dropped from comparison if either side is NaN there (paired drop).
    val dropped = (am.keySet ++ bm.keySet).filter(ts => isNaNAt(am, ts) || isNaNAt(bm, ts))
    val aTs = am.keySet -- dropped
    val bTs = bm.keySet -- dropped
    if (aTs != bTs) {
      val onlyA = (aTs -- bTs).toSeq.sorted.take(5)
      val onlyB = (bTs -- aTs).toSeq.sorted.take(5)
      Some(s"timestamps differ (after NaN drop): only-filodb=$onlyA only-prom=$onlyB; counts ${aTs.size} vs ${bTs.size}")
    } else {
      aTs.toSeq.sorted.collectFirst {
        case ts if !valuesMatch(am(ts), bm(ts), tol) => s"at ts $ts: ${am(ts)} != ${bm(ts)} (tol $tol)"
      }
    }
  }

  def compare(filodb: PromResult, prom: PromResult, tol: Tolerance, ignoreLabels: Boolean): CompareOutcome = {
    if (filodb.status != "success")
      return CompareOutcome(matched = false, s"FiloDB status=${filodb.status} error=${filodb.error}")
    if (prom.status != "success")
      return CompareOutcome(matched = false, s"Prometheus status=${prom.status} error=${prom.error}")

    val fs = filodb.series
    val ps = prom.series
    if (fs.isEmpty && ps.isEmpty) return CompareOutcome(matched = true, "both empty")
    if (fs.length != ps.length)
      return CompareOutcome(matched = false, s"series count ${fs.length} != ${ps.length}")

    if (ignoreLabels && fs.length == 1 && ps.length == 1) {
      pointsMatch(fs.head.points, ps.head.points, tol) match {
        case None => CompareOutcome(matched = true, "single-series value match")
        case Some(d) => CompareOutcome(matched = false, s"value mismatch: $d")
      }
    } else {
      val remaining = scala.collection.mutable.ListBuffer(ps: _*)
      for (f <- fs) {
        val fl = cleanLabels(f.metric)
        remaining.indexWhere(p => cleanLabels(p.metric) == fl) match {
          case -1 => return CompareOutcome(matched = false, s"no Prometheus series with labels $fl")
          case idx =>
            val p = remaining.remove(idx)
            pointsMatch(f.points, p.points, tol).foreach { d =>
              return CompareOutcome(matched = false, s"labels $fl: $d")
            }
        }
      }
      CompareOutcome(matched = true, s"${fs.length} series matched")
    }
  }
}
