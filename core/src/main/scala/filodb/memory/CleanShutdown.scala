package filodb.memory

/**
  * For clean shutdown using a Runtime hook.
  * The shutdown method is called by a Runtime hook
  */
trait CleanShutdown {

  def shutdown(): Unit

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      shutdown()
    }
  })
}
