package filodb.http.promcompat

import remote.RemoteStorage.{LabelPair, Sample, TimeSeries, WriteRequest}

final case class Series(metric: String, labels: Map[String, String], schema: String, valueColumn: String)
final case class Point(tsMillis: Long, value: Double)

final case class GeneratedData(series: Seq[(Series, Seq[Point])], startMillis: Long, endMillis: Long) {
  def startSec: Long = startMillis / 1000
  def endSec: Long = endMillis / 1000

  private def tagsCell(labels: Map[String, String]): String =
    (Seq("_ws_" -> DataGen.ws, "_ns_" -> DataGen.ns) ++ labels.toSeq)
      .map { case (k, v) => s"$k=$v" }.mkString(",")

  private def csvFor(metric: String, valueColumn: String): String = {
    val header = s"_metric_,tags,timestamp,$valueColumn"
    val rows = series.filter(_._1.metric == metric).flatMap { case (s, points) =>
      points.map(p => s"""$metric,"${tagsCell(s.labels)}",${p.tsMillis},${p.value}""")
    }
    (header +: rows).mkString("\n") + "\n"
  }

  def gaugeCsv: String = csvFor("my_gauge", "value")
  def counterCsv: String = csvFor("my_counter", "count")

  def writeRequest: WriteRequest = {
    val wr = WriteRequest.newBuilder
    series.foreach { case (s, points) =>
      val ts = TimeSeries.newBuilder
      ts.addLabels(LabelPair.newBuilder.setName("__name__").setValue(s.metric).build)
      ts.addLabels(LabelPair.newBuilder.setName("_ws_").setValue(DataGen.ws).build)
      ts.addLabels(LabelPair.newBuilder.setName("_ns_").setValue(DataGen.ns).build)
      s.labels.toSeq.sortBy(_._1).foreach { case (k, v) =>
        ts.addLabels(LabelPair.newBuilder.setName(k).setValue(v).build)
      }
      points.foreach(p => ts.addSamples(Sample.newBuilder.setValue(p.value).setTimestampMs(p.tsMillis).build))
      wr.addTimeseries(ts.build)
    }
    wr.build
  }
}

object DataGen {
  val ws = "test_ws"
  val ns = "promcompat"
  val stepMillis = 15000L
  private val modes = Seq("idle", "user", "system")
  private val instances = Seq("inst-a", "inst-b")

  def dataset(endMillis: Long, lookbackMinutes: Int = 30): GeneratedData = {
    val startMillis = endMillis - lookbackMinutes.toLong * 60000L
    val stamps = (startMillis to endMillis by stepMillis).toVector
    val series = for {
      (mode, mi) <- modes.zipWithIndex
      (inst, ii) <- instances.zipWithIndex
      metric <- Seq("my_gauge", "my_counter")
    } yield {
      val labels = Map("mode" -> mode, "instance" -> inst)
      val seriesIdx = mi * 2 + ii
      val (schema, valueColumn) =
        if (metric == "my_gauge") ("gauge", "value") else ("prom-counter", "count")
      val points = stamps.zipWithIndex.map { case (ts, i) =>
        val v =
          if (metric == "my_gauge") 10.0 * seriesIdx + (i % 7) * 1.5
          else 1000.0 + i.toLong * (seriesIdx + 1)
        Point(ts, v)
      }
      (Series(metric, labels, schema, valueColumn), points)
    }
    GeneratedData(series, startMillis, endMillis)
  }
}
