package filodb.akkabootstrapper.multijvm

import com.typesafe.config.{Config, ConfigFactory}


object ConsulBootstrapperMultiNodeConfig extends AkkaBootstrapperMultiNodeConfig {
  override def baseConfig : Config = ConfigFactory.parseString(
    s"""
       |akka-bootstrapper {
       |  seed-discovery.class = "filodb.akkabootstrapper.ConsulAkkaClusterSeedDiscovery"
       |  dns-srv.resolver-host = "127.0.0.1"  #consul by default
       |  dns-srv.resolver-port = 8600  # consul by default
       |  dns-srv.seed-node-count = 2
       |  dns-srv.service-name = "akkabootstrapper.service.consul"
       |  consul.api-host = "127.0.0.1"
       |  consul.api-port = 8500
       |}
     """.stripMargin).withFallback(super.baseConfig)

  commonConfig(baseConfig)

  nodeConfig(node1)(ConfigFactory.parseString(
    s"""
       |multijvmtest.http.port = 8070
     """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString(
    s"""
       |multijvmtest.http.port = 8080
     """.stripMargin))

  nodeConfig(node3)(ConfigFactory.parseString(
    s"""
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
  * Run test: `sbt "multi-jvm:test-only filodb.akkabootstrapper.multijvm.DnsSrvBootstrapperSpec"`
  */
class ConsulBootstrapperSpec extends MultiNodeSpec(ConsulBootstrapperMultiNodeConfig) with BaseAkkaBootstrapperSpec {
  override val akkaBootstrapperMultiNodeConfig = ConsulBootstrapperMultiNodeConfig
}
class ConsulBootstrapperSpecMultiJvmNode1 extends ConsulBootstrapperSpec
class ConsulBootstrapperSpecMultiJvmNode2 extends ConsulBootstrapperSpec
class ConsulBootstrapperSpecMultiJvmNode3 extends ConsulBootstrapperSpec
