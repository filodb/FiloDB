package filodb.core.memstore

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef
import filodb.core.store.StoreConfig

class AutoMemoryAllocUtilSpec extends AnyFunSpec with Matchers {

  // Builds a filodb-scoped config with automatic memory alloc settings.
  // Uses available-memory-bytes override so tests are deterministic (no actual JMX calls).
  private def makeConfig(
    enabled: Boolean = true,
    availableMemoryBytes: Long = 10L * 1024 * 1024 * 1024, // 10 GB
    nativePercent: Double = 24.0,
    blockPercent: Double  = 71.0,
    lucenePercent: Double  = 5.0,
    flightPercent: Double  = 0.0,
    numNodes: Int         = 2,
    overrideAvailableMemory: Boolean = true,
    serverFraction: Double = 0.6,
    clientFraction: Double = 0.4
  ): Config = {
    val availableMemLine =
      if (overrideAvailableMemory) s"available-memory-bytes = ${availableMemoryBytes}b" else ""
    ConfigFactory.parseString(
      s"""
         |filodb {
         |  min-num-nodes-in-cluster = $numNodes
         |  memstore.memory-alloc {
         |    automatic-alloc-enabled      = $enabled
         |    os-memory-needs              = 500MB
         |    native-memory-manager-percent = $nativePercent
         |    block-memory-manager-percent  = $blockPercent
         |    lucene-memory-percent         = $lucenePercent
         |    flight-rpc-memory-percent     = $flightPercent
         |    $availableMemLine
         |  }
         |  flight {
         |    server.fraction-allocator-limit = $serverFraction
         |    client.fraction-allocator-limit = $clientFraction
         |  }
         |}
         |""".stripMargin
    ).getConfig("filodb")
  }

  private def makeStoreConfig(shardMemPercent: Double = 100.0): StoreConfig =
    StoreConfig(ConfigFactory.parseString(
      s"""
         |flush-interval   = 10 minutes
         |shard-mem-size   = 100MB
         |shard-mem-percent = $shardMemPercent
         |""".stripMargin
    ))

  // ── isAutoMemoryConfigEnabled ───────────────────────────────────────────────

  describe("isAutoMemoryConfigEnabled") {

    it("returns false when automatic-alloc-enabled is false (percents not validated)") {
      val cfg = makeConfig(enabled = false)
      AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg) shouldEqual false
    }

    it("returns true when enabled and percents (no flight) sum exactly to 100") {
      val cfg = makeConfig(nativePercent = 24, blockPercent = 71, lucenePercent = 5, flightPercent = 0)
      AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg) shouldEqual true
    }

    it("returns true when enabled and percents including flight sum exactly to 100") {
      val cfg = makeConfig(nativePercent = 24, blockPercent = 66, lucenePercent = 5, flightPercent = 5)
      AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg) shouldEqual true
    }

    it("throws IllegalArgumentException when percents sum to less than 100") {
      val cfg = makeConfig(nativePercent = 24, blockPercent = 70, lucenePercent = 5, flightPercent = 0) // 99
      an[IllegalArgumentException] should be thrownBy {
        AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg)
      }
    }

    it("throws IllegalArgumentException when percents sum to more than 100") {
      val cfg = makeConfig(nativePercent = 30, blockPercent = 71, lucenePercent = 5, flightPercent = 0) // 106
      an[IllegalArgumentException] should be thrownBy {
        AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg)
      }
    }

    it("accepts percents that sum to 100 within floating-point tolerance (< 0.001 delta)") {
      // 24.333 + 70.667 + 5.0 + 0.0 = 100.000 exactly in FP arithmetic – close enough
      val cfg = makeConfig(nativePercent = 24.333, blockPercent = 70.667, lucenePercent = 5.0, flightPercent = 0.0)
      AutoMemoryAllocUtil.isAutoMemoryConfigEnabled(cfg) shouldEqual true
    }
  }

  // ── getFlightRPCMemoryAllocSize ─────────────────────────────────────────────

  describe("getFlightRPCMemoryAllocSize") {

    it("returns 0 when flight-rpc-memory-percent is 0") {
      val cfg = makeConfig(flightPercent = 0)
      AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg) shouldEqual 0L
    }

    it("computes the correct byte count from available memory and flight percent") {
      val availMem = 10L * 1024 * 1024 * 1024 // 10 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 66, lucenePercent = 5, flightPercent = 5
      )
      val expected = (availMem * 5.0 / 100).toLong
      AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg) shouldEqual expected
    }

    it("scales linearly with flight-rpc-memory-percent") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg10 = makeConfig(availableMemoryBytes = availMem,
        nativePercent = 19, blockPercent = 61, lucenePercent = 10, flightPercent = 10)
      val cfg20 = makeConfig(availableMemoryBytes = availMem,
        nativePercent = 19, blockPercent = 51, lucenePercent = 10, flightPercent = 20)

      val result10 = AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg10)
      val result20 = AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg20)
      result20 shouldEqual result10 * 2
    }
  }

  // ── getIngestionMemoryAllocSize ─────────────────────────────────────────────

  describe("getIngestionMemoryAllocSize") {

    it("computes the correct byte count from available memory and native percent") {
      val availMem = 10L * 1024 * 1024 * 1024 // 10 GB
      val cfg = makeConfig(availableMemoryBytes = availMem, nativePercent = 24)
      val expected = (availMem * 24.0 / 100).toLong
      AutoMemoryAllocUtil.getIngestionMemoryAllocSize(cfg) shouldEqual expected
    }

    it("scales linearly with native-memory-manager-percent") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg24 = makeConfig(availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 71, lucenePercent = 5, flightPercent = 0)
      val cfg48 = makeConfig(availableMemoryBytes = availMem,
        nativePercent = 48, blockPercent = 47, lucenePercent = 5, flightPercent = 0)

      val result24 = AutoMemoryAllocUtil.getIngestionMemoryAllocSize(cfg24)
      val result48 = AutoMemoryAllocUtil.getIngestionMemoryAllocSize(cfg48)
      result48 shouldEqual result24 * 2
    }

    it("uses the available-memory-bytes override rather than real system memory") {
      val override1 = 4L * 1024 * 1024 * 1024  // 4 GB
      val override2 = 8L * 1024 * 1024 * 1024  // 8 GB
      val nativePct  = 24.0

      val result1 = AutoMemoryAllocUtil.getIngestionMemoryAllocSize(
        makeConfig(availableMemoryBytes = override1, nativePercent = nativePct))
      val result2 = AutoMemoryAllocUtil.getIngestionMemoryAllocSize(
        makeConfig(availableMemoryBytes = override2, nativePercent = nativePct))

      result2 shouldEqual result1 * 2
      result1 shouldEqual (override1 * nativePct / 100).toLong
    }

    it("falls back to system memory when available-memory-bytes is not set, and result is positive") {
      // No available-memory-bytes override: reads containerMemory from JMX.
      val cfg = makeConfig(nativePercent = 24, overrideAvailableMemory = false)
      val result = AutoMemoryAllocUtil.getIngestionMemoryAllocSize(cfg)
      result should be > 0L
    }
  }

  // ── getPerShardBlockMemoryAllocSize ─────────────────────────────────────────

  describe("getPerShardBlockMemoryAllocSize") {

    it("distributes block memory evenly across shards per node") {
      val availMem  = 10L * 1024 * 1024 * 1024 // 10 GB
      val numNodes  = 2
      val numShards = 8
      val blockPct  = 71.0
      val shardPct  = 100.0

      val cfg         = makeConfig(availableMemoryBytes = availMem, blockPercent = blockPct, numNodes = numNodes)
      val storeConfig = makeStoreConfig(shardPct)
      val datasetRef  = DatasetRef("prometheus")

      // numShardsPerNode = ceil(8/2) = 4
      val numShardsPerNode = Math.ceil(numShards / numNodes.toDouble)
      val expected = (availMem * blockPct / 100 * shardPct / 100 / numShardsPerNode).toLong

      AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, numShards, datasetRef, storeConfig) shouldEqual expected
    }

    it("gives less memory per shard when the total number of shards increases") {
      val cfg        = makeConfig()
      val storeConf  = makeStoreConfig()
      val datasetRef = DatasetRef("prometheus")

      val result4  = AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, 4,  datasetRef, storeConf)
      val result8  = AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, 8,  datasetRef, storeConf)
      val result16 = AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, 16, datasetRef, storeConf)

      result4  should be > result8
      result8  should be > result16
    }

    it("halves per-shard memory when shardMemPercent is halved (within 1 byte of truncation)") {
      val availMem  = 10L * 1024 * 1024 * 1024
      val numShards = 4
      val cfg        = makeConfig(availableMemoryBytes = availMem)
      val datasetRef = DatasetRef("prometheus")

      val result100 = AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, numShards, datasetRef, makeStoreConfig(100.0))
      val result50  = AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, numShards, datasetRef, makeStoreConfig(50.0))

      // The two values are computed independently and both apply .toLong, so rounding
      // can differ by at most 1. Use a tolerance of 2 bytes instead of exact equality.
      (result50 * 2 - result100).abs should be <= 2L
    }

    it("uses ceiling when shards don't divide evenly across nodes") {
      // 7 shards / 3 nodes = 2.33 → ceil = 3 shards per node
      val availMem  = 6L * 1024 * 1024 * 1024 // 6 GB
      val numNodes  = 3
      val numShards = 7
      val blockPct  = 70.0
      val shardPct  = 100.0

      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        blockPercent = blockPct,
        lucenePercent = 5.0,
        nativePercent = 25.0,
        flightPercent = 0.0,
        numNodes = numNodes
      )
      val storeConfig = makeStoreConfig(shardPct)
      val datasetRef  = DatasetRef("prometheus")

      val numShardsPerNode = Math.ceil(numShards / numNodes.toDouble) // 3.0
      val expected = (availMem * blockPct / 100 * shardPct / 100 / numShardsPerNode).toLong

      AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, numShards, datasetRef, storeConfig) shouldEqual expected
    }

    it("correctly names the dataset in the log message (no exception thrown for any DatasetRef)") {
      val cfg        = makeConfig()
      val storeConf  = makeStoreConfig()

      // Exercise a few different dataset names – the method just uses them for logging
      Seq("prometheus", "metrics", "timeseries_ds").foreach { name =>
        noException should be thrownBy {
          AutoMemoryAllocUtil.getPerShardBlockMemoryAllocSize(cfg, 4, DatasetRef(name), storeConf)
        }
      }
    }
  }

  // ── getFlightServerMemoryAllocSize ──────────────────────────────────────────

  describe("getFlightServerMemoryAllocSize") {

    it("returns the server fraction of the total Flight RPC memory") {
      val availMem       = 10L * 1024 * 1024 * 1024 // 10 GB
      val flightPct      = 10.0
      val serverFraction = 0.6
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent  = 24, blockPercent = 61, lucenePercent = 5, flightPercent = flightPct,
        serverFraction = serverFraction
      )
      val expectedFlightRpc = (availMem * flightPct / 100).toLong
      val expected          = (serverFraction * expectedFlightRpc).toLong
      AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg) shouldEqual expected
    }

    it("returns 0 when flight-rpc-memory-percent is 0") {
      val cfg = makeConfig(flightPercent = 0, serverFraction = 0.6)
      AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg) shouldEqual 0L
    }

    it("returns 0 when server fraction-allocator-limit is 0.0") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.0, clientFraction = 1.0
      )
      AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg) shouldEqual 0L
    }

    it("equals the full Flight RPC size when server fraction-allocator-limit is 1.0") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 1.0, clientFraction = 0.0
      )
      val expectedFlightRpc = AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg)
      AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg) shouldEqual expectedFlightRpc
    }

    it("scales linearly with the server fraction-allocator-limit") {
      val availMem = 10L * 1024 * 1024 * 1024 // 10 GB
      val cfgHalf = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.5
      )
      val cfgQuarter = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.25
      )
      val resultHalf    = AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfgHalf)
      val resultQuarter = AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfgQuarter)
      // quarter should be half of half (within 1 byte rounding tolerance)
      (resultHalf / 2 - resultQuarter).abs should be <= 1L
    }

    it("scales linearly with the available memory") {
      val cfg4GB = makeConfig(
        availableMemoryBytes = 4L * 1024 * 1024 * 1024,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.6
      )
      val cfg8GB = makeConfig(
        availableMemoryBytes = 8L * 1024 * 1024 * 1024,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.6
      )
      val result4 = AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg4GB)
      val result8 = AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg8GB)
      // Two independent .toLong truncations can cause a 1-byte rounding gap
      (result8 - result4 * 2).abs should be <= 1L
    }
  }

  // ── getFlightClientMemoryAllocSize ──────────────────────────────────────────

  describe("getFlightClientMemoryAllocSize") {

    it("returns the client fraction of the total Flight RPC memory") {
      val availMem       = 10L * 1024 * 1024 * 1024 // 10 GB
      val flightPct      = 10.0
      val clientFraction = 0.4
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent  = 24, blockPercent = 61, lucenePercent = 5, flightPercent = flightPct,
        clientFraction = clientFraction
      )
      val expectedFlightRpc = (availMem * flightPct / 100).toLong
      val expected          = (clientFraction * expectedFlightRpc).toLong
      AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg) shouldEqual expected
    }

    it("returns 0 when flight-rpc-memory-percent is 0") {
      val cfg = makeConfig(flightPercent = 0, clientFraction = 0.4)
      AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg) shouldEqual 0L
    }

    it("returns 0 when client fraction-allocator-limit is 0.0") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 1.0, clientFraction = 0.0
      )
      AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg) shouldEqual 0L
    }

    it("equals the full Flight RPC size when client fraction-allocator-limit is 1.0") {
      val availMem = 8L * 1024 * 1024 * 1024 // 8 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.0, clientFraction = 1.0
      )
      val expectedFlightRpc = AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg)
      AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg) shouldEqual expectedFlightRpc
    }

    it("scales linearly with the client fraction-allocator-limit") {
      val availMem = 10L * 1024 * 1024 * 1024 // 10 GB
      val cfgHalf = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        clientFraction = 0.5
      )
      val cfgQuarter = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        clientFraction = 0.25
      )
      val resultHalf    = AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfgHalf)
      val resultQuarter = AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfgQuarter)
      // quarter should be half of half (within 1 byte rounding tolerance)
      (resultHalf / 2 - resultQuarter).abs should be <= 1L
    }

    it("scales linearly with the available memory") {
      val cfg4GB = makeConfig(
        availableMemoryBytes = 4L * 1024 * 1024 * 1024,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        clientFraction = 0.4
      )
      val cfg8GB = makeConfig(
        availableMemoryBytes = 8L * 1024 * 1024 * 1024,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        clientFraction = 0.4
      )
      val result4 = AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg4GB)
      val result8 = AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg8GB)
      // Two independent .toLong truncations can cause a 1-byte rounding gap
      (result8 - result4 * 2).abs should be <= 1L
    }

    it("server and client allocations sum to the total Flight RPC size when fractions sum to 1.0") {
      val availMem = 10L * 1024 * 1024 * 1024 // 10 GB
      val cfg = makeConfig(
        availableMemoryBytes = availMem,
        nativePercent = 24, blockPercent = 61, lucenePercent = 5, flightPercent = 10,
        serverFraction = 0.6, clientFraction = 0.4
      )
      val flightRpc = AutoMemoryAllocUtil.getFlightRPCMemoryAllocSize(cfg)
      val server    = AutoMemoryAllocUtil.getFlightServerMemoryAllocSize(cfg)
      val client    = AutoMemoryAllocUtil.getFlightClientMemoryAllocSize(cfg)
      // Both fractions apply independently to the same flightRpc total; rounding can cause a 1-byte gap.
      (server + client - flightRpc).abs should be <= 1L
    }
  }
}
