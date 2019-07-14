package filodb.core.memstore

object FiloSchedulers {
  val IngestSchedName = "ingestion-shard"
  val FlushSchedName = "flush-sched"
  val IOSchedName = "filodb.io"
  val QuerySchedName = "query-sched"
  val PopulateChunksSched = "populate-odp-chunks"

  def assertThreadName(name: String): Unit = {
    require(Thread.currentThread().getName.startsWith(name),
      s"Current thread expected to startWith $name but was ${Thread.currentThread().getName}")
  }
}
