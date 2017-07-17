package filodb.coordinator

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.scalalogging.slf4j.StrictLogging

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

abstract class ClusterSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with CoordinatorSetupWithFactory
  with StrictLogging
  with ImplicitSender with ScalaFutures
