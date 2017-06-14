
Initial sample user code to publish

```scala
val system = ActorSystem("systemName")
val router = ClusterRouter(system)

val kafkaConfig = Map()
router.provision(kafkaConfig)

while(events stream in) {
  router.publish(hashKey, protobufBytes)
}


Runtime.getRuntime().addShutdownHook(new Thread() {
  override def run(): Unit = super.run() {
    router.shutdown()
  }
})

```