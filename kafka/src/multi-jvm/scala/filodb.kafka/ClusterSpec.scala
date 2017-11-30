package filodb.kafka

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.FilodbCluster

abstract class ClusterSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with StrictLogging
  with ImplicitSender with ScalaFutures {

  val cluster = FilodbCluster(system)
}