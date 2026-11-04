package filodb.http.promcompat

import io.circe.{Json, JsonObject}

final case class PromPoint(tsSec: Long, value: Double)
final case class PromSeries(metric: Map[String, String], points: Seq[PromPoint])
final case class PromResult(status: String, resultType: Option[String],
                            series: Seq[PromSeries], error: Option[String])

object PromResult {
  private def toDouble(s: String): Double = s match {
    case "+Inf" => Double.PositiveInfinity
    case "-Inf" => Double.NegativeInfinity
    case "NaN"  => Double.NaN
    case other  => other.toDouble
  }

  private def point(arr: Vector[Json]): PromPoint = {
    val ts = arr(0).asNumber.map(_.toDouble.toLong).getOrElse(arr(0).asString.get.toLong)
    val v = arr(1).asString.map(toDouble).getOrElse(arr(1).asNumber.get.toDouble)
    PromPoint(ts, v)
  }

  private def metricMap(obj: JsonObject): Map[String, String] =
    obj("metric").flatMap(_.asObject).map(_.toMap.map { case (k, v) =>
      k -> v.asString.getOrElse(v.toString)
    }).getOrElse(Map.empty)

  def parse(json: String): PromResult = {
    val root = parse0(json)
    val status = root.hcursor.get[String]("status").getOrElse("error")
    if (status != "success") {
      val err = root.hcursor.get[String]("error").toOption
      return PromResult(status, None, Nil, err.orElse(Some("non-success status")))
    }
    val dataObj = root.hcursor.downField("data")
    val rt = dataObj.get[String]("resultType").toOption
    val resultJson = dataObj.downField("result").focus.getOrElse(Json.arr())
    // Determine the shape from the JSON, not the resultType label. FiloDB reports some full
    // aggregations (e.g. `sum(v)` without grouping) as resultType "scalar" yet serializes them
    // vector-shaped: an array of {metric, value} objects. A true Prometheus scalar is instead a
    // bare [ts, "val"] array. Parsing by shape handles both without relying on the label.
    val series = resultJson.asArray match {
      case Some(arr) if arr.exists(_.isObject) =>
        arr.flatMap(_.asObject).map { obj =>
          val pts = obj("values").flatMap(_.asArray)
            .map(_.map(a => point(a.asArray.get)))
            .orElse(obj("value").flatMap(_.asArray).map(a => Seq(point(a))))
            .getOrElse(Seq.empty)
          PromSeries(metricMap(obj), pts)
        }
      case Some(arr) if arr.nonEmpty =>
        Seq(PromSeries(Map.empty, Seq(point(arr))))
      case _ => Nil
    }
    PromResult(status, rt, series, None)
  }

  // NOTE: must be fully-qualified — inside `object PromResult`, an unqualified `parse`
  // resolves to this object's own `parse(json: String): PromResult` method (not circe's
  // parser), which would not compile (wrong return type) and would conceptually recurse.
  private def parse0(json: String): Json =
    io.circe.parser.parse(json).getOrElse(throw new IllegalArgumentException(s"Bad JSON: ${json.take(500)}"))
}
