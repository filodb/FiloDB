package filodb.core.store

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StoreConfigSpec extends AnyFunSpec with Matchers {
  // Minimal store block: flush-interval and shard-mem-size have no defaults and must be supplied.
  private def storeConf(extra: String): StoreConfig =
    StoreConfig(ConfigFactory.parseString(
      s"""
         |flush-interval = 1h
         |shard-mem-size = 100MB
         |$extra
         |""".stripMargin))

  describe("StoreConfig write-to-pk-ut-table") {
    it("should default to true when the key is absent (falls back to code default)") {
      storeConf("").writeToPkUTTable shouldEqual true
    }

    it("should honor a per-dataset override of write-to-pk-ut-table = false") {
      storeConf("write-to-pk-ut-table = false").writeToPkUTTable shouldEqual false
    }

    it("should honor an explicit write-to-pk-ut-table = true") {
      storeConf("write-to-pk-ut-table = true").writeToPkUTTable shouldEqual true
    }

    it("should round-trip writeToPkUTTable through toConfig") {
      val roundTripped = StoreConfig(storeConf("write-to-pk-ut-table = false").toConfig)
      roundTripped.writeToPkUTTable shouldEqual false
    }
  }

  describe("StoreConfig write-to-ingestion-time-index") {
    it("should default to true when the key is absent (falls back to code default)") {
      storeConf("").writeToIngestionTimeIndex shouldEqual true
    }

    it("should honor a per-dataset override of write-to-ingestion-time-index = false") {
      storeConf("write-to-ingestion-time-index = false").writeToIngestionTimeIndex shouldEqual false
    }

    it("should honor an explicit write-to-ingestion-time-index = true") {
      storeConf("write-to-ingestion-time-index = true").writeToIngestionTimeIndex shouldEqual true
    }

    it("should round-trip writeToIngestionTimeIndex through toConfig") {
      val roundTripped = StoreConfig(storeConf("write-to-ingestion-time-index = false").toConfig)
      roundTripped.writeToIngestionTimeIndex shouldEqual false
    }
  }
}
