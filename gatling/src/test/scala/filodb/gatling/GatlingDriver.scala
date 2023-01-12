package filodb.gatling

import io.gatling.app.Gatling
import io.gatling.core.config.GatlingPropertiesBuilder

object GatlingDriver extends App {
  val simClass = classOf[QueryRangeSimulation].getName
  val props = new GatlingPropertiesBuilder().simulationClass(simClass).runDescription(simClass)
  val tmp = System.getProperty("java.io.tmpdir")
  props.resultsDirectory(s"$tmp/results-$simClass-" + System.currentTimeMillis())
  Gatling.fromMap(props.build)
}
