<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Akka Cluster Bootstrapper](#akka-cluster-bootstrapper)
    - [Algorithm](#algorithm)
    - [Usage](#usage)
    - [Run Multi-JVM Tests](#run-multi-jvm-tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB

## Akka Cluster Bootstrapper

This library helps initialize an Akka Cluster on a compute cluster such as Mesos or Kubernetes. This is especially 
relevant when running the service within a container where the physical machine where the service will run is not 
known ahead of time and can change as redeployments are done.

### Algorithm

Every application that uses this library needs to expose two ports:
* Akka TCP Port for Akka Remoting
* Akka HTTP Port for HTTP API. This port is opened after the cluster bootstrapping is done. It will include the route 
  that advertises the cluster members.

Here are the sequence of steps each node in the cluster follows. It describes steps for the 
SimpleDnsSrvAkkaClusterSeedDiscovery discovery strategy. It assumes that each node of the application using this library 
gets registered with DNS and the hostnames and ports of the Akka TCP socket can be queried by retrieving the SRV records.
The steps below are the same for initial bootstrapping as well as scaled up nodes. 

1. Invoke HTTP GET for the "/seeds" path on load-balanced endpoint of the current application's Akka HTTP port. 
2. If we are able to fetch the list of seeds, then join cluster using the member list indicated, and go to step 6.
3. If HTTP call errors or times out, continuously poll DNS SRV records to discover peers of the current application. We 
   need the akka tcp hostname/port. 
4. Wait till a specific number of nodes are seen. The number of nodes to wait for is supplied in configuration. This 
   helps in identifying a single temporary leader wo will serve as the join node to prevent formation of more than 
   one cluster islands. See [more about this here](http://doc.akka.io/docs/akka/current/java/cluster-usage.html#joining-to-seed-nodes) 
5. Once the Akka TCP host and port of each peer is discovered, sort them in alphabetical order, and join the cluster 
   using this list. Sorting is required to ensure that everyone uses the same order of nodes for join. 
6. Once the node is part of the cluster, start the Akka HTTP server and include the route that advertises the current 
   members of the cluster as seeds. This endpoint will be used by subsequent nodes during scale-up.


With the above algorithm, nodes that are part of the initial bootstrap will wait for other nodes to start by doing the 
DNS SRV query and will then bootstrap the cluster. Once the cluster is formed, the application can then be scaled up 
and down. New nodes that come up after initial bootstrap will find the HTTP endpoint advertising the seeds alive and 
will join directly. 

### Usage

Include the library in your dependencies. 

Set up neccessary configuration in your application.conf. For full list of configuration and documentation, see the 
library's [reference.conf](../akka-bootstrapper/src/main/resources/reference.conf)

For fully working example code, look at the [example application](https://github.pie.apple.com/viswanathan-ramachandran/akka-bootstrapper-app). 
The key step is to invoke the AkkaBootstrapper.bootstrap() method, passing in the configuration and the Akka cluster 
object. This call blocks and waits until seedNodeCount peers are available and the join completes. The peer discovery 
strategy can be changed by providing an implementation of AkkaClusterSeedDiscovery class in the configuration. Included 
implementations are Simple SRV record discovery, Consul based service discovery, as well as explicitly listed nodes for 
non-clustered environments.  

Once the bootstrap() call returns, the caller can call the getAkkaHttpRoute() method to get an Akka HTTP Route which 
must be included in the application's HTTP server. The route advertises the seeds of the newly formed Akka cluster.

Subsequent nodes that come up first try to fetch the seeds for any existing cluster by visiting the seeds HTTP endpoint. 
Only if this is unavailable, a new cluster is formed.  

```scala

    val config = ConfigFactory.load()
    implicit val system = ActorSystem("ExampleAkkaCluster")
    val cluster = Cluster(system)
    try {
      val bootstrapper = AkkaBootstrapper(cluster, config)
      bootstrapper.bootstrap()
      implicit val materializer = ActorMaterializer()
      implicit val executionContext = system.dispatcher
      val akkaHttpHost = config.getString("example.http.host")
      val akkaHttpPort = config.getInt("example.http.port")
      Http().bindAndHandle(bootstrapper.getAkkaHttpRoute(), akkaHttpHost, akkaHttpPort).map { binding =>
        logger.info(s"Seeds http endpoint is up on this node")
      }
    } catch {
      case NonFatal(e) =>
        logger.error("Error occurred while setting up akka cluster", e)
        system.terminate()
    }

```

### Run Multi-JVM Tests

Setup Consul configuration
```
> cat /usr/local/etc/consul/config/basic_config.json 
{
"data_dir": "/usr/local/var/consul",
"ui" : true,
"dns_config" : {
    "enable_truncate" : true
}
 	
```

Then run consul consul agent in dev mode:
```
consul agent -dev -config-dir=/usr/local/etc/consul/config/
```

Run the multi-jvm tests:
```
sbt bootstrapper/multi-jvm:test
```
