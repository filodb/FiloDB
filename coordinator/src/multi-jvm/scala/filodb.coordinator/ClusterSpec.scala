package filodb.coordinator

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.FunSpecLike

import filodb.core.AsyncTest

abstract class ClusterSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with FunSpecLike with StrictLogging with ImplicitSender with AsyncTest {

  val cluster = FilodbCluster(system)
}
