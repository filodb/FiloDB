package filodb.coordinator

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.scalalogging.StrictLogging
import filodb.core.AsyncTest
import org.scalatest.funspec.AnyFunSpecLike

abstract class ClusterSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with AnyFunSpecLike with StrictLogging with ImplicitSender with AsyncTest {

  val cluster = FilodbCluster(system)
}
