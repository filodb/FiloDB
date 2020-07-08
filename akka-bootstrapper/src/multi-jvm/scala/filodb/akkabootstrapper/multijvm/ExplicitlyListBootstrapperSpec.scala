package filodb.akkabootstrapper.multijvm

import akka.remote.testkit.MultiNodeSpec
import com.typesafe.config.{Config, ConfigFactory}


object ExplicitlyListBootstrapperMultiNodeConfig extends AkkaBootstrapperMultiNodeConfig {

  override def baseConfig: Config = ConfigFactory.parseString(
    s"""
       |akka-bootstrapper {
       |  seed-discovery.class = "filodb.akkabootstrapper.ExplicitlyListClusterSeedDiscovery"
       |  explicitly-list.seeds = [
       |                          "akka.tcp://ExplicitlyListBootstrapperSpec@127.0.0.1:2552"
       |                          "akka.tcp://ExplicitlyListBootstrapperSpec@127.0.0.1:2562"
       |                          "akka.tcp://ExplicitlyListBootstrapperSpec@127.0.0.1:2572"
       |                    ]
       |}
     """.stripMargin).withFallback(super.baseConfig)

  commonConfig(baseConfig)

  nodeConfig(node1)(ConfigFactory.parseString(
    s"""
       |akka.remote.netty.tcp.port = 2552
       |multijvmtest.http.port = 8070
     """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString(
    s"""
       |akka.remote.netty.tcp.port = 2562
       |multijvmtest.http.port = 8080
     """.stripMargin))

  nodeConfig(node3)(ConfigFactory.parseString(
    s"""
       |akka.remote.netty.tcp.port = 2572
       |multijvmtest.http.port = 8090
     """.stripMargin))

}

/**
  * Run test as follows.
  *
  * Create consul dev config:
  * {{{
  *  > cat /usr/local/etc/consul/config/basic_config.json
  *  {
  *    "data_dir": "/usr/local/var/consul",
  *    "ui" : true,
  *    "dns_config" : {
  *        "enable_truncate" : true
  *     }
  *  }
  * }}}
  *
  * Then start consul: `consul agent -dev -config-dir=/usr/local/etc/consul/config/`
  *
  * Run test: `sbt "multi-jvm:test-only filodb.akkabootstrapper.multijvm.ExplicitlyListBootstrapperSpec"`
  */
class ExplicitlyListBootstrapperSpec extends MultiNodeSpec(ExplicitlyListBootstrapperMultiNodeConfig)
  with BaseAkkaBootstrapperSpec {
  override val akkaBootstrapperMultiNodeConfig = ExplicitlyListBootstrapperMultiNodeConfig
}
class ExplicitlyListBootstrapperSpecMultiJvmNode1 extends ExplicitlyListBootstrapperSpec
class ExplicitlyListBootstrapperSpecMultiJvmNode2 extends ExplicitlyListBootstrapperSpec
class ExplicitlyListBootstrapperSpecMultiJvmNode3 extends ExplicitlyListBootstrapperSpec
