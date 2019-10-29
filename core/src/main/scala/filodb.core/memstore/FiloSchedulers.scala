package filodb.core.memstore

import filodb.core.GlobalConfig

object FiloSchedulers {
  val IngestSchedName = "ingestion-shard"
  val FlushSchedName = "flush-sched"
  val IOSchedName = "filodb.io"
  val QuerySchedName = "query-sched"
  val PopulateChunksSched = "populate-odp-chunks"

  val assertEnabled = GlobalConfig.systemConfig.getBoolean("filodb.scheduler.enable-assertions")

  def assertThreadName(name: String): Unit = {
    if (assertEnabled) {
      require(Thread.currentThread().getName.startsWith(name),
        s"Current thread expected to startWith $name but was ${Thread.currentThread().getName}")
    }
  }
}
