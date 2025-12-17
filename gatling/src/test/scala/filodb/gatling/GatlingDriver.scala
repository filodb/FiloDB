package filodb.gatling

import io.gatling.app.Gatling

object GatlingDriver extends App {
  val simClass = classOf[QueryRangeSimulation].getName
  val tmp = System.getProperty("java.io.tmpdir")
  val resultsDir = s"$tmp/results-$simClass-" + System.currentTimeMillis()

  Gatling.main(Array(
    "-s", simClass,
    "-rd", simClass,
    "-rf", resultsDir
  ))
}
