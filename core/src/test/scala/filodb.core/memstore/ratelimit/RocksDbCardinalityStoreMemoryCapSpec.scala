package filodb.core.memstore.ratelimit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.MachineMetricsData
import filodb.core.memstore.ratelimit.RocksDbCardinalityStore._

class RocksDbCardinalityStoreMemoryCapSpec  extends AnyFunSpec with Matchers {

  val ref = MachineMetricsData.dataset2.ref

  val db = new RocksDbCardinalityStore(ref, 0)
  val tracker = new CardinalityTracker(ref, 0, 3, Seq(100, 100, 1000, 1000), db)

  it("should be able to write keys quickly and cap memory usage") {

    def dumpStats() = {
      println(db.statsAsString)
      println(s"memTablesSize=${db.memTablesSize}")
      println(s"blockCacheSize=${db.blockCacheSize}")
      println(s"diskSpaceUsed=${db.diskSpaceUsed}")
      println(s"estimatedNumKeys=${db.estimatedNumKeys}")
      println()
    }

    def assertStats() = {
      db.blockCacheSize should be < LRU_CACHE_SIZE
      (db.memTablesSize + db.blockCacheSize) should be < TOTAL_OFF_HEAP_SIZE
      db.diskSpaceUsed should be < (100L << 20)
    }

    val start = System.nanoTime()
    for { ws <- 0 until 5
          ns <- 0 until 20
          name <- 0 until 50
          ts <- 0 until 100 } {
      val mName = s"name_really_really_really_really_very_really_long_metric_name_$name"
      tracker.incrementCount(Seq( s"ws_prefix_$ws", s"ns_prefix_$ns", mName))
      if (name == 0 && ts ==0 ) assertStats()
    }
    val end = System.nanoTime()

    assertStats()
    dumpStats()
    val numTimeSeries = 5 * 20 * 100 * 50
    val totalTimeSecs = (end-start) / 1000000000L
    val timePerIncrementMicroSecs = (end-start) / numTimeSeries / 1000
    println(s"Was able to increment $numTimeSeries time series, $timePerIncrementMicroSecs" +
      s"us each increment total of ${totalTimeSecs}s")
    timePerIncrementMicroSecs should be < 100L

  }

}
