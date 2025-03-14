package filodb.downsampler

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import filodb.cardbuster.CardinalityBuster
import filodb.core.GlobalScheduler._
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder, RecordSchema}
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesShardStats, DownsampledTimeSeriesStore, OffHeapMemory}
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.memstore._
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.core.query.Filter.Equals
import filodb.core.query._
import filodb.core.store.{AllChunkScan, PartKeyRecord, SinglePartitionScan, StoreConfig, TimeRangeChunkScan}
import filodb.core.{DatasetRef, MachineMetricsData}
import filodb.downsampler.chunk.{BatchDownsampler, BatchExporter, Downsampler, DownsamplerSettings}
import filodb.downsampler.index.{DSIndexJobSettings, IndexJobDriver}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, DoubleVector, LongHistogram, Base2ExpHistogramBuckets}
import filodb.memory.format.{PrimitiveVectorReader, UnsafeUtils}
import filodb.query.exec._
import filodb.query.{QueryError, QueryResult}
import kamon.metric
import kamon.metric.Metric.Settings
import kamon.metric.{Histogram, Metric}
import kamon.tag.TagSet
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.apache.spark.sql.types._

import java.io.File
import java.time
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  // Add a path here to enable export during these tests. Useful for debugging export data.
  val exportToFile = None  // Some("s3a://bucket/directory/catalog/database/table")
  val exportConf =
    s"""{
       |  "filodb": { "downsampler": { "data-export": {
       |    "enabled": ${exportToFile.isDefined},
       |    "catalog": "",
       |    "database": "",
       |    "format": "iceberg",
       |    "options": {
       |        "distribution-mode": "none",
       |     }
       |    "key-labels": [_ns_],
       |    "groups": [
       |      {
       |        "key": ["_ns_=\\"my_ns\\""],
       |        "table": "",
       |        "table-path": "${exportToFile.getOrElse("")}",
       |        "label-column-mapping": [
       |         "_ns_", "namespace", "NOT NULL"
       |        ]
       |        "partition-by-columns": ["namespace"]
       |        "rules": [
       |          {
       |            "allow-filters": [],
       |            "block-filters": [],
       |            "drop-labels": []
       |          }
       |        ]
       |      }
       |    ]
       |  }}}
       |}
       |""".stripMargin

  val baseConf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))
  val conf = ConfigFactory.parseString(exportConf).withFallback(baseConf).resolve()
  val settings = new DownsamplerSettings(conf)
  val schemas = Schemas.fromConfig(settings.filodbConfig).get
  val queryConfig = QueryConfig(settings.filodbConfig.getConfig("query"))
  val dsIndexJobSettings = new DSIndexJobSettings(settings)
  val (dummyUserTimeStart, dummyUserTimeStop) = (123, 456)
  val batchDownsampler = new BatchDownsampler(settings, dummyUserTimeStart, dummyUserTimeStop)
  val batchExporter = new BatchExporter(settings, 123, 456)

  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  // NOTE: the following ws is disabled for using max-min columns. hence creating a separate tags map for it.
  val seriesTagsDisabledMaxMin = Map("_ws_".utf8 -> "disabled_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  val seriesTagsNaN = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8, "nan_support".utf8 -> "yes".utf8)
  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)

  val rawColStore = batchDownsampler.rawCassandraColStore
  val downsampleColStore = batchDownsampler.downsampleCassandraColStore

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram,
                                          Schemas.deltaCounter, Schemas.deltaHistogram, Schemas.untyped,
                                          Schemas.otelDeltaHistogram, Schemas.otelCumulativeHistogram),
                                     Map.empty, 100, rawDataStoreConfig)

  val shardInfo = TimeSeriesShardInfo(0, batchDownsampler.shardStats,
    offheapMem.bufferPools, offheapMem.nativeMemoryManager)

  val untypedName = "my_untyped"
  var untypedPartKeyBytes: Array[Byte] = _

  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _

  val counterName = "my_counter"
  var counterPartKeyBytes: Array[Byte] = _

  val deltaCounterName = "my_delta_counter"
  var deltaCounterPartKeyBytes: Array[Byte] = _

  val histName = "my_histogram"
  val histNameNaN = "my_histogram_NaN"
  var histPartKeyBytes: Array[Byte] = _
  var histNaNPartKeyBytes: Array[Byte] = _

  val deltaHistName = "my_delta_histogram"
  val otelExpDeltaHistName = "my_exp_delta_histogram"
  val deltaHistNameNaN = "my_histogram_NaN"
  var expDeltaHistPartKeyBytes: Array[Byte] = _
  var deltaHistPartKeyBytes: Array[Byte] = _
  var deltaHistNaNPartKeyBytes: Array[Byte] = _

  val gaugeLowFreqName = "my_gauge_low_freq"
  var gaugeLowFreqPartKeyBytes: Array[Byte] = _

  // otel-delta histogram
  val otelDeltaHistName = "my_otel_delta_histogram"
  var otelDeltaHistPartKeyBytes: Array[Byte] = _

  // otel-cumulative histogram
  val otelCummulativeHistName = "my_otel_cumulative_histogram"
  var otelCummulativeHistPartKeyBytes: Array[Byte] = _

  // prom-histogram and otel-cumulative histogram mixed data ingestion
  val histMixedSchemaCumulativeName = "my_legacy_otel_cumulative_histogram"
  var histMixedSchemaCumulativePartKeyBytes_legacy: Array[Byte] = _
  var histMixedSchemaCumulativePartKeyBytes_otel: Array[Byte] = _

  // delta-histogram and otel-delta histogram mixed data ingestion
  val histMixedSchemaDeltaName = "my_legacy_otel_delta_histogram"
  var histMixedSchemaDeltaPartKeyBytes_legacy: Array[Byte] = _
  var histMixedSchemaDeltaPartKeyBytes_otel: Array[Byte] = _

  val lastSampleTime = 74373042000L
  val pkUpdateHour = hour(lastSampleTime)

  val metricNames = Seq(gaugeName, gaugeLowFreqName, counterName, deltaCounterName, histName, deltaHistName,
    histNameNaN, untypedName, otelDeltaHistName, otelCummulativeHistName,
    histMixedSchemaCumulativeName, histMixedSchemaDeltaName)
  val shard = 0

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

  val currTime = System.currentTimeMillis()

  override def beforeAll(): Unit = {
    batchDownsampler.downsampleRefsByRes.values.foreach { ds =>
      downsampleColStore.initialize(ds, 4, settings.rawDatasetIngestionConfig.resources).futureValue
      downsampleColStore.truncate(ds, 4).futureValue
    }
    rawColStore.initialize(batchDownsampler.rawDatasetRef, 4, settings.rawDatasetIngestionConfig.resources).futureValue
    rawColStore.truncate(batchDownsampler.rawDatasetRef, 4).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it ("should export partitions according to the config") {

    val baseConf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))
      .withFallback(ConfigFactory.parseString(
        """
          |    filodb.downsampler.data-export {
          |      enabled = true
          |    }
          |""".stripMargin
      ))

    val onlyKeyConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a",
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val includeExcludeConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = [
        |                ["l2=\"l2a\""],
        |                ["l2=~\".*b\""]
        |              ]
        |              block-filters = [
        |                ["l2=\"l2c\""]
        |              ]
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val multiFilterConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = [
        |                [
        |                  "l2=\"l2a\"",
        |                  "l3=~\".*a\""
        |                ],
        |                [
        |                  "l2=\"l2a\"",
        |                  "l3=~\".*b\""
        |                ]
        |              ]
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val contradictFilterConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = [
        |                [
        |                  "l2=\"l2a\"",
        |                  "l2=~\".*b\""
        |                ]
        |              ]
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val multiKeyConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1", "l2"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\"", "l2=\"l2a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val multiRuleConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        },
        |        {
        |          key = ["l1=\"l1b\""]
        |          table = "l1b"
        |          table-path: "s3a://bucket/directory/catalog/database/l1b"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val multiRuleSameGroupConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = [["l3=\"l3a\""]]
        |              block-filters = [["l2=~\"l2(b|d)\""]]
        |              drop-labels = []
        |            },
        |            {
        |              allow-filters = [["l3=\"l3c\""], ["l2=\"l2c\""]]
        |              block-filters = [["l2=~\"l2(b|d)\""]]
        |              drop-labels = []
        |            },
        |            {
        |              allow-filters = [["l3=\"l3c\""], ["l2=\"l2c\""]]
        |              block-filters = [["l2=~\"l2(b|d)\""]]
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val multiRuleSameGroupConfBlocked = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["l1"]
        |      groups = [
        |        {
        |          key = ["l1=\"l1a\""]
        |          table = "l1a"
        |          table-path = "s3a://bucket/directory/catalog/database/l1a"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = [["l2=\"l2a\""]]  # allo A
        |              block-filters = []
        |              drop-labels = []
        |            },
        |            {
        |              allow-filters = [["l2=\"l2_DNE\""]]
        |              block-filters = [["l2=\"l2b\""]]  # block B (DNE allow filter to require C is matched below)
        |              drop-labels = []
        |            },
        |            {
        |              allow-filters = [["l2=\"l2c\""]] # allow C
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    )

    val allConfs = Seq(
      onlyKeyConf,
      includeExcludeConf,
      multiFilterConf,
      contradictFilterConf,
      multiKeyConf,
      multiRuleConf,
      multiRuleSameGroupConf,
      multiRuleSameGroupConfBlocked
    )

    val labelConfPairs = Seq(
      (Map("l1" -> "l1a", "l2" -> "l2a", "l3" -> "l3a"), Set[Config](onlyKeyConf,                  includeExcludeConf, multiKeyConf, multiRuleConf,   multiFilterConf, multiRuleSameGroupConf, multiRuleSameGroupConfBlocked)),
      (Map("l1" -> "l1a", "l2" -> "l2a", "l3" -> "l3b"), Set[Config](onlyKeyConf,                  includeExcludeConf, multiKeyConf, multiRuleConf,   multiFilterConf,                         multiRuleSameGroupConfBlocked)),
      (Map("l1" -> "l1a", "l2" -> "l2a", "l3" -> "l3c"), Set[Config](onlyKeyConf,                  includeExcludeConf, multiKeyConf, multiRuleConf,                    multiRuleSameGroupConf, multiRuleSameGroupConfBlocked)),
      (Map("l1" -> "l1a", "l2" -> "l2b", "l3" -> "l3a"), Set[Config](onlyKeyConf,                  includeExcludeConf,               multiRuleConf)),
      (Map("l1" -> "l1a", "l2" -> "l2c", "l3" -> "l3a"), Set[Config](onlyKeyConf, multiRuleConf,                                                                       multiRuleSameGroupConf, multiRuleSameGroupConfBlocked)),
      (Map("l1" -> "l1a", "l2" -> "l2d", "l3" -> "l3a"), Set[Config](onlyKeyConf, multiRuleConf)),
      (Map("l1" -> "l1b", "l2" -> "l2a", "l3" -> "l3a"), Set[Config](             multiRuleConf)),
      (Map("l1" -> "l1c", "l2" -> "l2a", "l3" -> "l3a"), Set[Config]()),
    )

    allConfs.foreach { conf =>
      val dsSettings = new DownsamplerSettings(conf.withFallback(baseConf))
      val batchExporter = new BatchExporter(dsSettings, dummyUserTimeStart, dummyUserTimeStop)
      // make sure batchExporter correctly decides when to export
      labelConfPairs.foreach { case (partKeyMap, includedConf) =>
        val shouldExport = dsSettings.exportKeyToConfig.exists { case (keyFilters, exportTableConfig) =>
          batchExporter.getRuleIfShouldExport(partKeyMap, keyFilters, exportTableConfig).isDefined
        }
        shouldExport shouldEqual includedConf.contains(conf)
      }
    }
  }

  it("should give correct export-column index of column name") {
    val testConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = [
        |      "_ws_",
        |      "_ns_",
        |      "metric",
        |      "labels",
        |      "epoch_timestamp",
        |      "timestamp",
        |      "value",
        |      "year",
        |      "month",
        |      "day",
        |      "hour"]
        |      groups = [
        |        {
        |          key = ["_ws_=\"my_ws\""]
        |          table = "my_ws"
        |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    ).withFallback(conf)

    val dsSettings = new DownsamplerSettings(testConf.withFallback(conf))
    val batchExporter = BatchExporter(dsSettings, dummyUserTimeStart, dummyUserTimeStop)
    dsSettings.exportRuleKey.zipWithIndex.map{case (colName, i) =>
      batchExporter.getColumnIndex(colName, dsSettings.exportKeyToConfig.head._2).get shouldEqual i}
  }

  it("should correctly apply export key column filters") {
    {
      val testConf = ConfigFactory.parseString(
        """
          |    filodb.downsampler.data-export {
          |      key-labels = ["_ws_", "_ns_"]
          |      groups = [
          |        {
          |          key = ["_ws_=\"my_ws\""]
          |          table = "my_ws"
          |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
          |          label-column-mapping = [
          |            "_ws_", "workspace", "NOT NULL",
          |            "_ns_", "namespace", "NOT NULL"
          |          ]
          |          partition-by-columns = []
          |          rules = [
          |              {
          |                  allow-filters = []
          |                  block-filters = []
          |                  drop-labels = []
          |              }
          |          ]
          |        }
          |      ]
          |    }
          |""".stripMargin
      ).withFallback(conf)
      val settings = new DownsamplerSettings(testConf)
      val rows = Seq(
        Map("_ws_" -> "my_ws", "_ns_" -> "a"),
        Map("_ws_" -> "DNE", "_ns_" -> "b"),
        Map("_ws_" -> "my_ws", "_ns_" -> "c"),
        Map("_ws_" -> "DNE", "_ns_" -> "d"),
      )
      val expected = Seq(
        Map("_ws_" -> "my_ws", "_ns_" -> "a"),
        Map("_ws_" -> "my_ws", "_ns_" -> "c"),
      )
      val filtered = rows.filter{ row =>
        val rule = batchExporter.getRuleIfShouldExport(row,
          settings.exportKeyToConfig.head._1,
          settings.exportKeyToConfig.head._2)
        rule.isDefined
      }
      filtered shouldEqual expected
    }

    {
      val testConf = ConfigFactory.parseString(
        """
          |    filodb.downsampler.data-export {
          |      key-labels = ["_ws_", "_ns_"]
          |      groups = [
          |        {
          |          key = ["_ns_=~\"my_ns.*\""]
          |          table = "my_ws"
          |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
          |          label-column-mapping = [
          |            "_ws_", "workspace", "NOT NULL",
          |            "_ns_", "namespace", "NOT NULL"
          |          ]
          |          partition-by-columns = []
          |          rules = [
          |              {
          |                  allow-filters = []
          |                  block-filters = []
          |                  drop-labels = []
          |              }
          |          ]
          |        }
          |      ]
          |    }
          |""".stripMargin
      ).withFallback(conf)
      val settings = new DownsamplerSettings(testConf)
      val rows = Seq(
        Map("_ws_" -> "a", "_ns_" -> "my_ns"),
        Map("_ws_" -> "b", "_ns_" -> "DNE"),
        Map("_ws_" -> "c", "_ns_" -> "my_ns123"),
        Map("_ws_" -> "d", "_ns_" -> "DNE"),
      )
      val expected = Seq(
        Map("_ws_" -> "a", "_ns_" -> "my_ns"),
        Map("_ws_" -> "c", "_ns_" -> "my_ns123"),
      )
      val filtered = rows.filter { row =>
        val rule = batchExporter.getRuleIfShouldExport(row,
          settings.exportKeyToConfig.head._1,
          settings.exportKeyToConfig.head._2)
        rule.isDefined
      }
      filtered shouldEqual expected
    }

    {
      val testConf = ConfigFactory.parseString(
        """
          |    filodb.downsampler.data-export {
          |      key-labels = ["_ws_", "_ns_"]
          |      groups = [
          |        {
          |          key = ["_ws_=\"my_ws\"", "_ns_=~\"my_ns.*\""]
          |          table = "my_ws"
          |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
          |          label-column-mapping = [
          |            "_ws_", "workspace", "NOT NULL",
          |            "_ns_", "namespace", "NOT NULL"
          |          ]
          |          partition-by-columns = []
          |          rules = [
          |              {
          |                  allow-filters = []
          |                  block-filters = []
          |                  drop-labels = []
          |              }
          |          ]
          |        }
          |      ]
          |    }
          |""".stripMargin
      ).withFallback(conf)
      val settings = new DownsamplerSettings(testConf)
      val rows = Seq(
        Map("_ws_" -> "my_ws", "_ns_" -> "my_ns1"),
        Map("_ws_" -> "my_ws123", "_ns_" -> "my_ns2"),
        Map("_ws_" -> "c", "_ns_" -> "c"),
        Map("_ws_" -> "my_ws", "_ns_" -> "DNE"),
        Map("_ws_" -> "my_ws", "_ns_" -> "my_ns3"),
      )
      val expected = Seq(
        Map("_ws_" -> "my_ws", "_ns_" -> "my_ns1"),
        Map("_ws_" -> "my_ws", "_ns_" -> "my_ns3"),
      )
      val filtered = rows.filter { row =>
        val rule = batchExporter.getRuleIfShouldExport(row,
          settings.exportKeyToConfig.head._1,
          settings.exportKeyToConfig.head._2)
        rule.isDefined
      }
      filtered shouldEqual expected
    }

    {
      val testConf = ConfigFactory.parseString(
        """
          |    filodb.downsampler.data-export {
          |      key-labels = ["_ws_", "_ns_"]
          |      groups = [
          |        {
          |          key = ["_ns_=~\"my_ns.*\"", "_ws_=~\"my_ws.*\""]
          |          table = "my_ws"
          |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
          |          label-column-mapping = [
          |            "_ws_", "workspace", "NOT NULL",
          |            "_ns_", "namespace", "NOT NULL"
          |          ]
          |          partition-by-columns = []
          |          rules = [
          |              {
          |                  allow-filters = []
          |                  block-filters = []
          |                  drop-labels = []
          |              }
          |          ]
          |        }
          |      ]
          |    }
          |""".stripMargin
      ).withFallback(conf)
      val settings = new DownsamplerSettings(testConf)
      val rows = Seq(
        Map("_ws_" -> "my_ws1", "_ns_" -> "my_ns1"),
        Map("_ws_" -> "b", "_ns_" -> "b"),
        Map("_ws_" -> "my_ws1", "_ns_" -> "DNE"),
        Map("_ws_" -> "DNE", "_ns_" -> "my_ns1"),
        Map("_ws_" -> "my_ws2", "_ns_" -> "my_ns2"),
      )
      val expected = Seq(
        Map("_ws_" -> "my_ws1", "_ns_" -> "my_ns1"),
        Map("_ws_" -> "my_ws2", "_ns_" -> "my_ns2"),
      )
      val filtered = rows.filter { row =>
        val rule = batchExporter.getRuleIfShouldExport(row,
          settings.exportKeyToConfig.head._1,
          settings.exportKeyToConfig.head._2)
        rule.isDefined
      }
      filtered shouldEqual expected
    }
  }

  it("should give correct export schema") {
    val testConf = ConfigFactory.parseString(
      """
        |    filodb.downsampler.data-export {
        |      key-labels = ["_ws_"]
        |      groups = [
        |        {
        |          key = ["_ws_=\"my_ws\""]
        |          table = "my_ws"
        |          table-path = "s3a://bucket/directory/catalog/database/my_ws"
        |          label-column-mapping = [
        |            "_ws_", "workspace", "NOT NULL",
        |            "_ns_", "namespace", "NOT NULL"
        |          ]
        |          partition-by-columns = ["namespace"]
        |          rules = [
        |            {
        |              allow-filters = []
        |              block-filters = []
        |              drop-labels = []
        |            }
        |          ]
        |        }
        |      ]
        |    }
        |""".stripMargin
    ).withFallback(conf)

    val dsSettings = new DownsamplerSettings(testConf.withFallback(conf))
    val exportSchema = {
      val fields = new scala.collection.mutable.ArrayBuffer[StructField](11)
      fields.append(
        StructField("workspace", StringType, false),
        StructField("namespace", StringType, false),
        StructField("metric", StringType, false),
        StructField("labels", MapType(StringType, StringType)),
        StructField("epoch_timestamp", LongType, false),
        StructField("timestamp", TimestampType, false),
        StructField("value", DoubleType),
        StructField("year", IntegerType, false),
        StructField("month", IntegerType, false),
        StructField("day", IntegerType, false),
        StructField("hour", IntegerType, false))
      StructType(fields)
    }
    dsSettings.exportKeyToConfig.head._2.tableSchema shouldEqual exportSchema
  }

  it ("should write untyped data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.untyped)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.untyped, untypedName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.untyped, partKey, shardInfo, 1)

    untypedPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, untypedName, seriesTags),
      Seq(74372802000L, 5d, untypedName, seriesTags),

      Seq(74372861000L, 9d, untypedName, seriesTags),
      Seq(74372862000L, 11d, untypedName, seriesTags),

      Seq(74372921000L, 13d, untypedName, seriesTags),
      Seq(74372922000L, 15d, untypedName, seriesTags),

      Seq(74372981000L, 17d, untypedName, seriesTags),
      Seq(74372982000L, 15d, untypedName, seriesTags),

      Seq(74373041000L, 13d, untypedName, seriesTags),
      Seq(74373042000L, 11d, untypedName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.untyped.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(untypedPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey, shardInfo,1)

    gaugePartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      Seq(74372861000L, 9d, gaugeName, seriesTags),
      Seq(74372862000L, 11d, gaugeName, seriesTags),

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      Seq(74372981000L, 17d, gaugeName, seriesTags),
      Seq(74372982000L, 15d, gaugeName, seriesTags),

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(gaugePartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write low freq gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeLowFreqName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey, shardInfo, 1)

    gaugeLowFreqPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      // skip next minute

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      // skip next minute

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(gaugeLowFreqPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write prom counter data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promCounter)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, counterName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promCounter, partKey, shardInfo, 1)

    counterPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, counterName, seriesTags),
      Seq(74372801500L, 4d, counterName, seriesTags),
      Seq(74372802000L, 5d, counterName, seriesTags),

      Seq(74372861000L, 9d, counterName, seriesTags),
      Seq(74372861500L, 10d, counterName, seriesTags),
      Seq(74372862000L, 11d, counterName, seriesTags),

      Seq(74372921000L, 2d, counterName, seriesTags),
      Seq(74372921500L, 7d, counterName, seriesTags),
      Seq(74372922000L, 15d, counterName, seriesTags),

      Seq(74372981000L, 17d, counterName, seriesTags),
      Seq(74372981500L, 1d, counterName, seriesTags),
      Seq(74372982000L, 15d, counterName, seriesTags),

      Seq(74373041000L, 18d, counterName, seriesTags),
      Seq(74373042000L, 20d, counterName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promCounter.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(counterPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it("should write delta counter data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.deltaCounter)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.deltaCounter, deltaCounterName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.deltaCounter, partKey, shardInfo, 1)

    deltaCounterPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, deltaCounterName, seriesTags),
      Seq(74372801500L, 1d, deltaCounterName, seriesTags),
      Seq(74372802000L, 1d, deltaCounterName, seriesTags),

      Seq(74372861000L, 4d, deltaCounterName, seriesTags),
      Seq(74372861500L, 1d, deltaCounterName, seriesTags),
      Seq(74372862000L, 1d, deltaCounterName, seriesTags),

      Seq(74372921000L, 2d, deltaCounterName, seriesTags),
      Seq(74372921500L, 5d, deltaCounterName, seriesTags),
      Seq(74372922000L, 8d, deltaCounterName, seriesTags),

      Seq(74372981000L, 2d, deltaCounterName, seriesTags),
      Seq(74372981500L, 1d, deltaCounterName, seriesTags),
      Seq(74372982000L, 14d, deltaCounterName, seriesTags),

      Seq(74373041000L, 3d, deltaCounterName, seriesTags),
      Seq(74373042000L, 2d, deltaCounterName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.deltaCounter.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(deltaCounterPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it("should write additional prom counter partitionKeys with start/endtimes overlapping with original partkey - " +
      "to test partkeyIndex marge logic") {

    val rawDataset = Dataset("prometheus", Schemas.promCounter)
    val startTime = 74372801000L
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, counterName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promCounter, partKey, shardInfo, 1)

    counterPartKeyBytes = part.partKeyBytes
    val pk1 = PartKeyRecord(counterPartKeyBytes, startTime - 3600000, currTime, shard)
    val pk2 = PartKeyRecord(counterPartKeyBytes, startTime + 3600000, currTime, shard)
    val pk3 = PartKeyRecord(counterPartKeyBytes, startTime, currTime + 3600000, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk1), 259200, pkUpdateHour + 1).futureValue
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk2), 259200, pkUpdateHour + 3).futureValue
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk3), 259200, pkUpdateHour + 2).futureValue
  }

  it ("should write prom histogram data to cassandra") {
    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, histName, seriesTags)
    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey, shardInfo, 1)
    histPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histName, seriesTags),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histName, seriesTags),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histName, seriesTags),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), histName, seriesTags),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histName, seriesTags),
      Seq(74372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), histName, seriesTags),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), histName, seriesTags),
      Seq(74372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), histName, seriesTags),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), histName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), histName, seriesTags),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), histName, seriesTags),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), histName, seriesTags),
      Seq(74373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), histName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(histPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it("should write delta histogram data to cassandra") {
    val rawDataset = Dataset("prometheus", Schemas.deltaHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.deltaHistogram, deltaHistName, seriesTags)
    val part = new TimeSeriesPartition(0, Schemas.deltaHistogram, partKey, shardInfo, 1)
    deltaHistPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), deltaHistName, seriesTags),
      Seq(74372801500L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 2, 2)), deltaHistName, seriesTags),
      Seq(74372802000L, 3d, 3d, LongHistogram(bucketScheme, Array(2L, 3, 3)), deltaHistName, seriesTags),

      Seq(74372861000L, 4d, 3d, LongHistogram(bucketScheme, Array(0L, 0, 3)), deltaHistName, seriesTags),
      Seq(74372861500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), deltaHistName, seriesTags),
      Seq(74372862000L, 1d, 4d, LongHistogram(bucketScheme, Array(0L, 3, 4)), deltaHistName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), deltaHistName, seriesTags),
      Seq(74372921500L, 5d, 7d, LongHistogram(bucketScheme, Array(1L, 1, 7)), deltaHistName, seriesTags),
      Seq(74372922000L, 8d, 10d, LongHistogram(bucketScheme, Array(0L, 8, 10)), deltaHistName, seriesTags),

      Seq(74372981000L, 2d, 2d, LongHistogram(bucketScheme, Array(1L, 1, 2)), deltaHistName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), deltaHistName, seriesTags),
      Seq(74372982000L, 14d, 14d, LongHistogram(bucketScheme, Array(0L, 14, 14)), deltaHistName, seriesTags),

      Seq(74373041000L, 3d, 4d, LongHistogram(bucketScheme, Array(1L, 1, 4)), deltaHistName, seriesTags),
      Seq(74373042000L, 2d, 6d, LongHistogram(bucketScheme, Array(3L, 4, 6)), deltaHistName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.deltaHistogram.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(deltaHistPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write otel cumulative histogram to cassandra") {
    val rawDataset = Dataset("prometheus", Schemas.otelCumulativeHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.otelCumulativeHistogram, otelCummulativeHistName, seriesTags)
    val part = new TimeSeriesPartition(0, Schemas.otelCumulativeHistogram, partKey, shardInfo, 1)
    otelCummulativeHistPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, min, max, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), 0d, 10d, otelCummulativeHistName, seriesTags),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), 0d, 20d, otelCummulativeHistName, seriesTags),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), 1d, 30d, otelCummulativeHistName, seriesTags),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), 2d, 15d, otelCummulativeHistName, seriesTags),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), 1d, 10d, otelCummulativeHistName, seriesTags),
      Seq(74372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), 1d, 18d, otelCummulativeHistName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), 0d, 10d, otelCummulativeHistName, seriesTags),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), 1d, 20d, otelCummulativeHistName, seriesTags),
      Seq(74372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), 1d, 30d, otelCummulativeHistName, seriesTags),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), 2d, 25d, otelCummulativeHistName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), 0d, 10d, otelCummulativeHistName, seriesTags),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), 0d, 15d, otelCummulativeHistName, seriesTags),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), 1d, 30d, otelCummulativeHistName, seriesTags),
      Seq(74373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), 2d, 40d, otelCummulativeHistName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.otelCumulativeHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(otelCummulativeHistPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write otel delta histogram to cassandra") {
    val rawDataset = Dataset("prometheus", Schemas.otelDeltaHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.otelDeltaHistogram, otelDeltaHistName, seriesTags)
    val part = new TimeSeriesPartition(0, Schemas.otelDeltaHistogram, partKey, shardInfo, 1)
    otelDeltaHistPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, min, max, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), 0d, 10d, otelDeltaHistName, seriesTags),
      Seq(74372801500L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 2, 2)), 1d, 20d, otelDeltaHistName, seriesTags),
      Seq(74372802000L, 3d, 3d, LongHistogram(bucketScheme, Array(2L, 3, 3)), 2d, 15d, otelDeltaHistName, seriesTags),

      Seq(74372861000L, 4d, 3d, LongHistogram(bucketScheme, Array(0L, 0, 3)), 1d, 16d, otelDeltaHistName, seriesTags),
      Seq(74372861500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), 0d, 10d, otelDeltaHistName, seriesTags),
      Seq(74372862000L, 1d, 4d, LongHistogram(bucketScheme, Array(0L, 3, 4)), 0d, 30d, otelDeltaHistName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), 0d, 11d, otelDeltaHistName, seriesTags),
      Seq(74372921500L, 5d, 7d, LongHistogram(bucketScheme, Array(1L, 1, 7)), 3d, 20d, otelDeltaHistName, seriesTags),
      Seq(74372922000L, 8d, 10d, LongHistogram(bucketScheme, Array(0L, 8, 10)), 6d, 25d, otelDeltaHistName, seriesTags),

      Seq(74372981000L, 2d, 2d, LongHistogram(bucketScheme, Array(1L, 1, 2)), 1d, 20d, otelDeltaHistName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), 0d, 10d, otelDeltaHistName, seriesTags),
      Seq(74372982000L, 14d, 14d, LongHistogram(bucketScheme, Array(0L, 14, 14)), 3d, 25d, otelDeltaHistName, seriesTags),

      Seq(74373041000L, 3d, 4d, LongHistogram(bucketScheme, Array(1L, 1, 4)), 1d, 30d, otelDeltaHistName, seriesTags),
      Seq(74373042000L, 2d, 6d, LongHistogram(bucketScheme, Array(3L, 4, 6)), 1d, 22d, otelDeltaHistName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.otelDeltaHistogram.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(otelDeltaHistPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write prom-histogram and otel-cumulative-histogram (mixed mode) data to cassandra") {
    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    // Step 1: Write prom histogram data
    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin)
    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey, shardInfo, 1)
    histMixedSchemaCumulativePartKeyBytes_legacy = part.partKeyBytes
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin)
    )
    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(histMixedSchemaCumulativePartKeyBytes_legacy, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue

    // Step 2: Write otel cumulative histogram data
    val rawDataset2 = Dataset("prometheus", Schemas.otelCumulativeHistogram)
    val partBuilder2 = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey2 = partBuilder2.partKeyFromObjects(Schemas.otelCumulativeHistogram, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin)
    val part2 = new TimeSeriesPartition(0, Schemas.otelCumulativeHistogram, partKey2, shardInfo, 1)
    histMixedSchemaCumulativePartKeyBytes_otel = part2.partKeyBytes
    val rawSamples2 = Stream( // time, sum, count, hist, min, max, name, tags
      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), 1d, 10d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), 3d, 20d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), 4d, 30d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), 5d, 30d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), 0d, 20d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), 4d, 32d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), 0d, 20d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin),
      Seq(74373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), 5d, 25d, histMixedSchemaCumulativeName, seriesTagsDisabledMaxMin)
    )
    MachineMetricsData.records(rawDataset2, rawSamples2).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.otelCumulativeHistogram.ingestionSchema, base, offset)
      part2.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part2.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks2 = part2.makeFlushChunks(offheapMem.blockMemFactory)
    rawColStore.write(rawDataset2.ref, Observable.fromIteratorUnsafe(chunks2)).futureValue
    val pk2 = PartKeyRecord(histMixedSchemaCumulativePartKeyBytes_otel, 74372921000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset2.ref, shard, Observable.now(pk2), 259200, pkUpdateHour).futureValue
  }

  it ("should write delta-histogram and otel-delta-histogram (mixed mode) data to cassandra") {
    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    // Step 1: Write delta histogram data
    val rawDataset = Dataset("prometheus", Schemas.deltaHistogram)
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.deltaHistogram, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin)
    val part = new TimeSeriesPartition(0, Schemas.deltaHistogram, partKey, shardInfo, 1)
    histMixedSchemaDeltaPartKeyBytes_legacy = part.partKeyBytes
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372801500L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 2, 2)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372802000L, 3d, 3d, LongHistogram(bucketScheme, Array(2L, 3, 3)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),

      Seq(74372861000L, 4d, 3d, LongHistogram(bucketScheme, Array(0L, 0, 3)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372861500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372862000L, 1d, 4d, LongHistogram(bucketScheme, Array(0L, 3, 4)), histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
    )
    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.deltaHistogram.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(histMixedSchemaDeltaPartKeyBytes_legacy, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue

    // Step 2: Write otel delta histogram data
    val rawDataset2 = Dataset("prometheus", Schemas.otelDeltaHistogram)
    val partBuilder2 = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey2 = partBuilder2.partKeyFromObjects(Schemas.otelDeltaHistogram, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin)
    val part2 = new TimeSeriesPartition(0, Schemas.otelDeltaHistogram, partKey2, shardInfo, 1)
    histMixedSchemaDeltaPartKeyBytes_otel = part2.partKeyBytes
    val rawSamples2 = Stream( // time, sum, count, hist, min, max, name, tags
      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), 0d, 11d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372921500L, 5d, 7d, LongHistogram(bucketScheme, Array(1L, 1, 7)), 3d, 20d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372922000L, 8d, 10d, LongHistogram(bucketScheme, Array(0L, 8, 10)), 6d, 25d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),

      Seq(74372981000L, 2d, 2d, LongHistogram(bucketScheme, Array(1L, 1, 2)), 1d, 20d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), 0d, 10d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74372982000L, 14d, 14d, LongHistogram(bucketScheme, Array(0L, 14, 14)), 3d, 25d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),

      Seq(74373041000L, 3d, 4d, LongHistogram(bucketScheme, Array(1L, 1, 4)), 1d, 30d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin),
      Seq(74373042000L, 2d, 6d, LongHistogram(bucketScheme, Array(3L, 4, 6)), 1d, 22d, histMixedSchemaDeltaName, seriesTagsDisabledMaxMin)
    )
    MachineMetricsData.records(rawDataset2, rawSamples2).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.otelDeltaHistogram.ingestionSchema, base, offset)
      part2.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part2.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks2 = part2.makeFlushChunks(offheapMem.blockMemFactory)
    rawColStore.write(rawDataset2.ref, Observable.fromIteratorUnsafe(chunks2)).futureValue
    val pk2 = PartKeyRecord(histMixedSchemaDeltaPartKeyBytes_otel, 74372921000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset2.ref, shard, Observable.now(pk2), 259200, pkUpdateHour).futureValue
  }

  it("should write otel exponential delta histogram data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.otelDeltaHistogram)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.otelDeltaHistogram, otelExpDeltaHistName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.otelDeltaHistogram, partKey, shardInfo, 1)

    expDeltaHistPartKeyBytes = part.partKeyBytes

    val bucketScheme1 = Base2ExpHistogramBuckets(3, -1, 3)
    val bucketScheme2 = Base2ExpHistogramBuckets(2, -1, 3)
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme1, Array(0L, 0L, 0, 1)), 3d, 5d, otelExpDeltaHistName, seriesTags),
      Seq(74372801500L, 2d, 2d, LongHistogram(bucketScheme1, Array(0L, 0L, 2, 2)), 4d, 6d, otelExpDeltaHistName, seriesTags),
      Seq(74372802000L, 3d, 3d, LongHistogram(bucketScheme2, Array(0L, 2L, 3, 3)), 5d, 7d, otelExpDeltaHistName, seriesTags),

      Seq(74372861000L, 4d, 3d, LongHistogram(bucketScheme2, Array(0L, 0L, 0, 3)), 3d, 4d, otelExpDeltaHistName, seriesTags),
      Seq(74372861500L, 1d, 1d, LongHistogram(bucketScheme2, Array(0L, 0L, 0, 1)), 3d, 4d, otelExpDeltaHistName, seriesTags),
      Seq(74372862000L, 1d, 4d, LongHistogram(bucketScheme2, Array(0L, 0L, 3, 4)), 3d, 4d, otelExpDeltaHistName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme1, Array(0L, 0L, 0, 2)), 43d, 134d, otelExpDeltaHistName, seriesTags),
      Seq(74372921500L, 5d, 7d, LongHistogram(bucketScheme1, Array(0L, 1L, 1, 7)), 63d, 214d, otelExpDeltaHistName, seriesTags),
      Seq(74372922000L, 8d, 10d, LongHistogram(bucketScheme2, Array(0L, 0L, 8, 10)), 13d, 384d, otelExpDeltaHistName, seriesTags),

      Seq(74372981000L, 2d, 2d, LongHistogram(bucketScheme2, Array(0L, 1L, 1, 2)), 53d, 74d, otelExpDeltaHistName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme2, Array(0L, 0L, 1, 1)), 23d, 64d, otelExpDeltaHistName, seriesTags),
      Seq(74372982000L, 14d, 14d, LongHistogram(bucketScheme2, Array(0L, 0L, 14, 14)), 63d, 94d, otelExpDeltaHistName, seriesTags),

      Seq(74373041000L, 3d, 4d, LongHistogram(bucketScheme1, Array(0L, 1L, 1, 4)), 13d, 334d, otelExpDeltaHistName, seriesTags),
      Seq(74373042000L, 2d, 6d, LongHistogram(bucketScheme1, Array(0L, 3L, 4, 6)), 13d, 94d, otelExpDeltaHistName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.otelDeltaHistogram.ingestionSchema, base, offset)
      part.ingest(lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(deltaHistPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  it ("should write prom histogram data with NaNs to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, histNameNaN, seriesTagsNaN)

    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey, shardInfo, 1)

    histNaNPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histNameNaN, seriesTagsNaN),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histNameNaN, seriesTagsNaN),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histNameNaN, seriesTagsNaN),
      Seq(74372802500L, Double.NaN, Double.NaN, LongHistogram(bucketScheme, Array(0L, 0, 0)), histNameNaN, seriesTagsNaN),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), histNameNaN, seriesTagsNaN),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histNameNaN, seriesTagsNaN),
      Seq(74372862000L, Double.NaN, Double.NaN, LongHistogram(bucketScheme, Array(0L, 0, 0)), histNameNaN, seriesTagsNaN),
      Seq(74372862500L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histNameNaN, seriesTagsNaN),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), histNameNaN, seriesTagsNaN),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), histNameNaN, seriesTagsNaN),
      Seq(74372922000L, Double.NaN, Double.NaN, LongHistogram(bucketScheme, Array(0L, 0, 0)), histNameNaN, seriesTagsNaN),
      Seq(74372922500L, 4d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), histNameNaN, seriesTagsNaN),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), histNameNaN, seriesTagsNaN),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), histNameNaN, seriesTagsNaN),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), histNameNaN, seriesTagsNaN),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), histNameNaN, seriesTagsNaN),
      Seq(74373041500L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), histNameNaN, seriesTagsNaN),
      Seq(74373042000L, Double.NaN, Double.NaN, LongHistogram(bucketScheme, Array(0L, 0, 0)), histNameNaN, seriesTagsNaN)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
    val pk = PartKeyRecord(histNaNPartKeyBytes, 74372801000L, currTime, shard)
    rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  val numShards = dsIndexJobSettings.numShards
  val bulkPkUpdateHours = {
    val start = pkUpdateHour / 6 * 6 // 6 is number of hours per downsample chunk
    start until start + 6
  }

  it("should simulate bulk part key records being written into raw for migration") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val schemas = Seq(Schemas.promHistogram, Schemas.gauge, Schemas.promCounter)
    case class PkToWrite(pkr: PartKeyRecord, updateHour: Long)
    val pks = for { i <- 0 to 10000 } yield {
      val schema = schemas(i % schemas.size)
      val partKey = partBuilder.partKeyFromObjects(schema, s"bulkmetric$i", bulkSeriesTags)
      val bytes = schema.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey)
      PkToWrite(PartKeyRecord(bytes, i, i + 500, i % numShards),
        bulkPkUpdateHours(i % bulkPkUpdateHours.size))
    }

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    pks.groupBy(k => (k.pkr.shard, k.updateHour)).foreach { case ((shard, updHour), shardPks) =>
      rawColStore.writePartKeys(rawDataset.ref, shard, Observable.fromIterable(shardPks).map(_.pkr),
        259200, updHour).futureValue
    }
  }

  it ("should free all offheap memory") {
    offheapMem.free()
  }

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    val downsampler = new Downsampler(settings)
    downsampler.run(sparkConf).close()
  }

  it ("should migrate partKey data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.index.timeInPeriodOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    val indexUpdater = new IndexJobDriver(settings, dsIndexJobSettings)
    indexUpdater.run(sparkConf).close()
  }

  it ("should verify migrated partKey data and match the downsampled schema") {

    def pkMetricSchemaReader(pkr: PartKeyRecord): (String, String) = {
      val schemaId = RecordSchema.schemaID(pkr.partKey, UnsafeUtils.arayOffset)
      val partSchema = batchDownsampler.schemas(schemaId)
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      (strPairs.find(p => p._1 == "_metric_").get._2, partSchema.data.name)
    }

    val metrics = Set((counterName, Schemas.promCounter.name),
      (deltaCounterName, Schemas.deltaCounter.name),
      (gaugeName, Schemas.dsGauge.name),
      (gaugeLowFreqName, Schemas.dsGauge.name),
      (histName, Schemas.promHistogram.name),
      (histNameNaN, Schemas.promHistogram.name),
      (deltaHistName, Schemas.deltaHistogram.name),
      (otelDeltaHistName, Schemas.otelDeltaHistogram.name),
      (otelCummulativeHistName, Schemas.otelCumulativeHistogram.name),
      (histMixedSchemaCumulativeName, Schemas.promHistogram.name),
      (histMixedSchemaCumulativeName, Schemas.otelCumulativeHistogram.name),
      (histMixedSchemaDeltaName, Schemas.deltaHistogram.name),
      (histMixedSchemaDeltaName, Schemas.otelDeltaHistogram.name),
    )
    val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")), 0)
    val tsSchemaMetric = Await.result(partKeys.map(pkMetricSchemaReader).toListL.runToFuture, 1 minutes)
    tsSchemaMetric.filter(k => metricNames.contains(k._1)).toSet shouldEqual metrics
  }

  it("should verify bulk part key record migration and validate completeness of PK migration") {

    def pkMetricName(pkr: PartKeyRecord): (String, PartKeyRecord) = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      (strPairs.find(p => p._1 == "_metric_").head._2, pkr)
    }
    val readPartKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
                                                     shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }

    // partkey start/endtimes are merged are overridden by the latest partkey record read from raw cluster.
    val startTime = 74372801000L
    val readKeys = readPartKeys.map(_._1).toSet
    val counterPartkey = readPartKeys.filter(_._1 == counterName).head._2
    counterPartkey.startTime shouldEqual startTime
    counterPartkey.endTime shouldEqual currTime + 3600000

    // readKeys should not contain untyped part key - we dont downsample untyped
    readKeys shouldEqual (0 to 10000).map(i => s"bulkmetric$i").toSet ++ (metricNames.toSet - untypedName)
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, 1.minute.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372862000L, 9.0, 11.0, 20.0, 2.0, 10.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74372982000L, 15.0, 17.0, 32.0, 2.0, 16.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  /*
  Tip: After running this spec, you can bring up the local downsample server and hit following URL on browser
  http://localhost:9080/promql/prometheus/api/v1/query_range?query=my_gauge%7B_ws_%3D%27my_ws%27%2C_ns_%3D%27my_ns%27%7D&start=74372801&end=74373042&step=10&verbose=true&spread=2
   */

  it("should read and verify low freq gauge in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugeLowFreqPartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugeLowFreqPartKeyBytes,
                                                                          batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugeLowFreqPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, 1.minute.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual dsGaugeLowFreqPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, 1.minute.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),
      (74372802000L, 5d),

      (74372862000L, 11d),

      (74372921000L, 2d),
      (74372922000L, 15d),

      (74372981000L, 17d),
      (74372981500L, 1d),
      (74372982000L, 15d),

      (74373042000L, 20d)
    )
  }

  it("should read and verify delta counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    println("delta partkey::" + Schemas.deltaCounter.partKeySchema.toHexString(deltaCounterPartKeyBytes, UnsafeUtils.arayOffset))
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(deltaCounterPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.deltaCounter.downsample.get, 0, 0,
      downsampledPartData1, 1.minute.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual deltaCounterPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader

    // drops wont be detected
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual false

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    /*
    74372801000L, 3d,
    74372801500L, 1d,
    74372802000L, 1d,  --> 5

    74372861000L, 4d,
    74372861500L, 1d,
    74372862000L, 1d, --> 6

    74372921000L, 2d,
    74372921500L, 5d,
    74372922000L, 8d, --> 15

    74372981000L, 2d,
    74372981500L, 1d,
    74372982000L, 14d, --> 17

    74373041000L, 3d,
    74373042000L, 2d, --> 5
     */

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372802000L, 5d),
      (74372862000L, 6d),
      (74372922000L, 15d),
      (74372982000L, 17d),
      (74373042000L, 5d),
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d)),

      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),

      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (74372922000L, 15d, 19d, Seq(1d, 15d, 19d)),

      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (74372982000L, 15d, 15d, Seq(0d, 15d, 15d)),

      (74373042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )
  }

  it("should read and verify delta histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(deltaHistPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.deltaHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual deltaHistPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual false

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    /*
    74372801000L, 0d, 1d, Array(0L, 0, 1))
    74372801500L, 2d, 2d, Array(0L, 2, 2))
    74372802000L, 3d, 3d, Array(2L, 3, 3)) -> 74372802000,5.0,6.0,Vector(2.0, 5.0, 6.0)

    74372861000L, 4d, 3d, Array(0L, 0, 3))
    74372861500L, 1d, 1d, Array(0L, 0, 1))
    74372862000L, 1d, 4d, Array(0L, 3, 4)) -> 74372862000,6.0,8.0,Vector(0.0, 3.0, 8.0)

    74372921000L, 2d, 2d, Array(0L, 0, 2))
    74372921500L, 5d, 7d, Array(1L, 1, 7))
    74372922000L, 8d, 10d, Array(0L, 8, 10)) -> 74372922000,15.0,19.0,Vector(1.0, 9.0, 19.0)

    74372981000L, 2d, 2d, Array(1L, 1, 2))
    74372981500L, 1d, 1d, Array(0L, 1, 1))
    74372982000L, 14d, 14d, Array(0L, 14, 14)) -> 74372982000,17.0,17.0,Vector(1.0, 16.0, 17.0)

    74373041000L, 3d, 4d, Array(1L, 1, 4))
    74373042000L, 2d, 6d, Array(3L, 4, 6)) -> 74373042000,5.0,10.0,Vector(4.0, 5.0, 10.0)
     */


    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d)),
      (74372862000L, 6d, 8d, Seq(0d, 3d, 8d)),
      (74372922000L, 15d, 19d, Seq(1d, 9d, 19d)),
      (74372982000L, 17d, 17d, Seq(1d, 16d, 17d)),
      (74373042000L, 5d, 10d, Seq(4d, 5d, 10d))
    )
  }

  it("should read and verify otel cummulative histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
        batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
        0,
        SinglePartitionScan(otelCummulativeHistPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.otelCumulativeHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual otelCummulativeHistPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)),
        r.getDouble(4),r.getDouble(5))
    }.toList

    // time, sum, count, histogram, min, max
    downsampledData1 shouldEqual Seq(
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d), 0d, 10d),
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d), 0d, 30d),

      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d), 1d, 18d),

      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d), 0d, 10d),
      (74372922000L, 15d, 19d, Seq(1d, 15d, 19d), 1d, 30d),

      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d), 2d, 25d),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d), 0d, 10d),
      (74372982000L, 15d, 15d, Seq(0d, 15d, 15d), 0d, 15d),

      (74373042000L, 20d, 25d, Seq(4d, 20d, 25d), 1d, 40d)
    )
  }

  it("should read and verify otel delta histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
        batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
        0,
        SinglePartitionScan(otelDeltaHistPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.otelDeltaHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual otelDeltaHistPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual false

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)),
        r.getDouble(4), r.getDouble(5))
    }.toList

    /*
    74372801000L, 0d, 1d, Array(0L, 0, 1), 0d, 10d)
    74372801500L, 2d, 2d, Array(0L, 2, 2), 1d, 20d)
    74372802000L, 3d, 3d, Array(2L, 3, 3), 2d, 15d) -> 74372802000,5.0,6.0,Vector(2.0, 5.0, 6.0), 0d (min), 20d (max)

    74372861000L, 4d, 3d, Array(0L, 0, 3), 1d, 16d)
    74372861500L, 1d, 1d, Array(0L, 0, 1), 0d, 10d)
    74372862000L, 1d, 4d, Array(0L, 3, 4), 0d, 30d) -> 74372862000,6.0,8.0,Vector(0.0, 3.0, 8.0), 0d (min), 30d (max)

    74372921000L, 2d, 2d, Array(0L, 0, 2), 0d, 11d)
    74372921500L, 5d, 7d, Array(1L, 1, 7), 3d, 20d)
    74372922000L, 8d, 10d, Array(0L, 8, 10), 6d, 25d) -> 74372922000,15.0,19.0,Vector(1.0, 9.0, 19.0), 0d (min), 25d (max)

    74372981000L, 2d, 2d, Array(1L, 1, 2), 1d, 20d)
    74372981500L, 1d, 1d, Array(0L, 1, 1), 0d, 10d)
    74372982000L, 14d, 14d, Array(0L, 14, 14), 3d, 25d) -> 74372982000,17.0,17.0,Vector(1.0, 16.0, 17.0), 0d, 25d

    74373041000L, 3d, 4d, Array(1L, 1, 4), 1d, 30d)
    74373042000L, 2d, 6d, Array(3L, 4, 6), 1d, 22d) -> 74373042000,5.0,10.0,Vector(4.0, 5.0, 10.0), 1d, 30d
     */


    // time, sum, count, histogram, min, max
    downsampledData1 shouldEqual Seq(
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d), 0d, 20d),
      (74372862000L, 6d, 8d, Seq(0d, 3d, 8d), 0d, 30d),
      (74372922000L, 15d, 19d, Seq(1d, 9d, 19d), 0d, 25d),
      (74372982000L, 17d, 17d, Seq(1d, 16d, 17d), 0d, 25d),
      (74373042000L, 5d, 10d, Seq(4d, 5d, 10d), 1d, 30d)
    )
  }

  it("should read and verify exponential delta histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
        batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
        0,
        SinglePartitionScan(expDeltaHistPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.otelDeltaHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual expDeltaHistPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual false

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3).asInstanceOf[LongHistogram]
      val b = h.buckets.asInstanceOf[Base2ExpHistogramBuckets]
      (r.getLong(0), r.getDouble(1), r.getDouble(2), b, h.values.toSeq, r.getDouble(4), r.getDouble(5))
    }.toList

    // time, sum, count, histogram, bucket values
    // new sample for every bucket-scheme and downsamplePeriod combination
    downsampledData1 shouldEqual Seq(
      (74372801500L,2.0,3.0, Base2ExpHistogramBuckets(3, -1, 3), Seq(0, 0, 2, 3), 3d, 6d),
      (74372802000L,3.0,3.0,Base2ExpHistogramBuckets(2, -1, 3), Seq(0, 0, 3, 8), 5d, 7d),
      (74372861750L,6.0,8.0,Base2ExpHistogramBuckets(2, -1, 3), Seq(0, 0, 3, 8), 3d, 4d),
      (74372921500L,7.0,9.0,Base2ExpHistogramBuckets(3, -1, 3), Seq(0, 1, 1, 9), 43d, 214d),
      (74372922000L,8.0,10.0,Base2ExpHistogramBuckets(2, -1, 3), Seq(0, 1, 16, 17), 13d, 384d),
      (74372982000L,17.0,17.0,Base2ExpHistogramBuckets(2, -1, 3), Seq(0, 1, 16, 17), 23d, 94d),
      (74373042000L,5.0,10.0,Base2ExpHistogramBuckets(3, -1, 3), Seq(0, 4, 5, 10), 13d, 334d)
    )
  }

  it("should read and verify prom histogram data with NaNs in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(histNaNPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual histNaNPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    val acc = ctrChunkInfo.vectorAccessor(2)
    val addr = ctrChunkInfo.vectorAddress(2)
    DoubleVector(acc, addr).dropPositions(acc, addr).toList shouldEqual Seq(2, 4, 6, 8, 11, 14)
    PrimitiveVectorReader.dropped(acc, addr) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    val expected = Seq(
      (74372801000L, 0d, 1d, Vector(0d, 0d, 1d)),
      (74372802000L, 5d, 6d, Vector(2d, 5d, 6d)),
      (74372802500L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop(2)

      (74372861500L, 10.0, 10.0, Vector(2.0, 5.0, 10.0)),
      (74372862000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop(4)
      (74372862500L, 11.0, 14.0, Vector(2.0, 8.0, 14.0)),

      (74372921000L, 2d, 2d, Vector(0d, 0d, 2d)), // drop (6)
      (74372921500L, 7.0, 9.0, Vector(1.0, 7.0, 9.0)),
      (74372922000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop (8)
      (74372922500L, 4.0, 1.0, Vector(0.0, 1.0, 1.0)),

      (74372981000L, 17d, 21d, Vector(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Vector(0d, 1d, 1d)), // drop (11)
      (74372982000L, 15d, 15d, Vector(0d, 15d, 15d)),

      (74373041500L, 20d, 25d, Vector(4d, 20d, 25d)),
      (74373042000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)) // drop (14)
    )
    // time, sum, count, histogram
    downsampledData1.filter(_._2.isNaN).map(_._1) shouldEqual expected.filter(_._2.isNaN).map(_._1) // timestamp of NaN records should match
    downsampledData1.filter(!_._2.isNaN) shouldEqual expected.filter(!_._2.isNaN) // Non NaN records should match
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData2 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData2, 5.minutes.toMillis.toInt)

    downsampledPart2.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5),
      new AtomicLong(), Long.MaxValue, "query-id")

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (74372982000L, 3.0, 17.0, 88.0, 8.0, 11.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1),
      new AtomicLong(), Long.MaxValue, "query-id")

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),

      (74372862000L, 11d),

      (74372921000L, 2d),

      (74372981000L, 17d),
      (74372981500L, 1d),

      (74372982000L, 15.0d),

      (74373042000L, 20.0d)
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    val acc = ctrChunkInfo.vectorAccessor(2)
    val addr = ctrChunkInfo.vectorAddress(2)
    DoubleVector(acc, addr).dropPositions(acc, addr).toList shouldEqual Seq(2, 4)
    PrimitiveVectorReader.dropped(acc, addr) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),
      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)), // drop (2)
      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)), // drop (4)
      (74372982000L, 15.0d, 15.0d, Seq(0.0, 15.0, 15.0)),
      (74373042000L, 20.0d, 25.0d, Seq(4.0, 20.0, 25.0))
    )
  }

  it("should read and verify prom histogram data with NaN in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(histNaNPartKeyBytes))
      .toListL.runToFuture.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, 5.minutes.toMillis.toInt)

    downsampledPart1.partKeyBytes shouldEqual histNaNPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    val acc = ctrChunkInfo.vectorAccessor(2)
    val addr = ctrChunkInfo.vectorAddress(2)
    DoubleVector(acc, addr).dropPositions(acc, addr).toList shouldEqual Seq(2, 4, 6, 8, 10, 13)
    PrimitiveVectorReader.dropped(acc, addr) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3),
      new AtomicLong(), Long.MaxValue, "query-id")

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    val expected = Seq(
      (74372801000L, 0d, 1d, Vector(0d, 0d, 1d)),
      (74372802000L, 5.0, 6.0, Vector(2.0, 5.0, 6.0)),
      (74372802500L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop (2)

      (74372861500L, 10.0, 10.0, Vector(2.0, 5.0, 10.0)),
      (74372862000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop (4)
      (74372862500L, 11d, 14d, Vector(2d, 8d, 14d)),

      (74372921000L, 2d, 2d, Vector(0d, 0d, 2d)), // drop (6)
      (74372921500L, 7.0, 9.0, Vector(1.0, 7.0, 9.0)),
      (74372922000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)), // drop (8)

      (74372981000L, 17d, 21d, Vector(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Vector(0d, 1d, 1d)), // drop 10
      (74372982000L, 15.0d, 15.0d, Vector(0.0, 15.0, 15.0)),
      (74373041500L, 20.0d, 25.0d, Vector(4.0, 20.0, 25.0)),
      (74373042000L, Double.NaN, Double.NaN, Vector(0.0, 0.0, 0.0)) // drop (13)
    )
    // time, sum, count, histogram
    downsampledData1.filter(_._2.isNaN).map(_._1) shouldEqual expected.filter(_._2.isNaN).map(_._1) // timestamp of NaN records should match
    downsampledData1.filter(!_._2.isNaN) shouldEqual expected.filter(!_._2.isNaN) // Non NaN records should match
  }

  it("should bring up DownsampledTimeSeriesShard and be able to read data using SelectRawPartitionsExec") {
    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)
    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, settings.rawDatasetIngestionConfig.downsampleConfig)
    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    def testQuery(expectedRes: Int, metricName: String, colFilters: Seq[ColumnFilter]): Unit = {
      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(metricName))
      val exec = MultiSchemaPartitionsExec(
        QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000))),
        InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), batchDownsampler.rawDatasetRef, 0, queryFilters,
        TimeRangeChunkScan(74372801000L, 74373042000L), "_metric_")

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual expectedRes
      res.result.foreach(_.rows.nonEmpty shouldEqual true)
    }

    // test query for each metric name.
    Seq(gaugeName, gaugeLowFreqName, counterName, histNameNaN, histName,
      otelDeltaHistName, otelCummulativeHistName).foreach { metricName =>
      val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
      val colFiltersNaN = seriesTagsNaN.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
      val columnFilters = if (metricName == histNameNaN) colFiltersNaN else colFilters
      testQuery(1, metricName, columnFilters)
    }

    // test query for *Mixed Histogram* schema. This should not throw "SchemaMismatch" error
    Seq(histMixedSchemaCumulativeName, histMixedSchemaDeltaName).foreach { metricName =>
      val colFilters = seriesTagsDisabledMaxMin.map{ case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
      // we should get two srv here because we have two different schemas
      testQuery(2, metricName, colFilters)
    }

    downsampleTSStore.shutdown()
  }

  ignore("should bring up DownsampledTimeSeriesShard and not rebuild index") {

    var indexFolder = new File("target/tmp/downsample-index")
    if (indexFolder.exists()) {
      FileUtils.deleteDirectory(indexFolder)
    }

    val durableIndexConf = ConfigFactory.parseFile(
      new File("conf/timeseries-filodb-durable-downsample-index-server.conf")
    ).resolve()

    val durableIndexSettings = new DownsamplerSettings(durableIndexConf)

    val downsampleTSStore = new DownsampledTimeSeriesStore(
      downsampleColStore, rawColStore,
      durableIndexSettings.filodbConfig
    )

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, durableIndexSettings.rawDatasetIngestionConfig.downsampleConfig)

    val recoveredRecords = downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue
    recoveredRecords shouldBe 5
    val fromHour = hour(74372801000L*1000)
    val toHour = hour(74373042000L*1000)
    downsampleTSStore.refreshIndexForTesting(batchDownsampler.rawDatasetRef, fromHour, toHour)
    downsampleTSStore.shutdown()

    val downsampleTSStore2 = new DownsampledTimeSeriesStore(
      downsampleColStore, rawColStore,
      durableIndexSettings.filodbConfig
    )

    downsampleTSStore2.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, durableIndexSettings.rawDatasetIngestionConfig.downsampleConfig)

    val recoveredRecords2 = downsampleTSStore2.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue
    recoveredRecords2 shouldBe 0

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
    val colFiltersNaN = seriesTagsNaN.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

    Seq(gaugeName, gaugeLowFreqName, counterName, histNameNaN, histName).foreach { metricName =>
      val colFltrs = if (metricName == histNameNaN) colFiltersNaN else colFilters
      val queryFilters = colFltrs :+ ColumnFilter("_metric_", Equals(metricName))
      val exec = MultiSchemaPartitionsExec(
        QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000))),
        InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), batchDownsampler.rawDatasetRef, 0, queryFilters,
        TimeRangeChunkScan(74372801000L, 74373042000L), "_metric_")

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore2, querySession)(queryScheduler)
        .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 1
      res.result.foreach(_.rows.nonEmpty shouldEqual true)
    }

    downsampleTSStore2.shutdown()
  }

  it("should bring up DownsampledTimeSeriesShard and be able to read data PeriodicSeriesMapper") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(counterName))
    val qc = QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000)))
    val exec = MultiSchemaPartitionsExec(qc,
      InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), batchDownsampler.rawDatasetRef, 0, queryFilters,
      AllChunkScan, "_metric_")
    // window should be auto-extended to 10m
    exec.addRangeVectorTransformer(PeriodicSamplesMapper(74373042000L, 10, 74373042000L,Some(610000),
      Some(InternalRangeFunction.Rate)))

    val querySession = QuerySession(QueryContext(), queryConfig)
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
      .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryResult]
    queryScheduler.shutdown()

    res.result.size shouldEqual 1
    res.result.foreach(_.rows.nonEmpty shouldEqual true)
    downsampleTSStore.shutdown()
  }

  it("should encounter error when doing increase on DownsampledTimeSeriesShard when lookback < 10m resolution ") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(counterName))
    val qc = QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000)))
    val exec = MultiSchemaPartitionsExec(qc,
      InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), batchDownsampler.rawDatasetRef, 0, queryFilters,
      AllChunkScan, "_metric_")
    exec.addRangeVectorTransformer(PeriodicSamplesMapper(74373042000L, 10, 74373042000L,Some(290000),
      Some(InternalRangeFunction.Increase)))

    val querySession = QuerySession(QueryContext(), queryConfig)
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
      .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryError]
    queryScheduler.shutdown()

    // exception thrown because lookback is < downsample data resolution of 5m
    res.t.isInstanceOf[IllegalArgumentException] shouldEqual true
    downsampleTSStore.shutdown()

  }

  it("should bring up DownsampledTimeSeriesShard and NOT be able to read untyped data using SelectRawPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(untypedName))
      val exec = MultiSchemaPartitionsExec(
        QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000))),
        InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig),
        batchDownsampler.rawDatasetRef, 0,
        queryFilters, AllChunkScan, "_metric_")

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 0
    downsampleTSStore.shutdown()

  }

  it("should bring up DownsampledTimeSeriesShard and be able to read specific columns " +
      "from gauge using MultiSchemaPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)
    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, 1, settings.rawDatasetIngestionConfig.downsampleConfig)
    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue
    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(gaugeName))
    val exec = MultiSchemaPartitionsExec(
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 1000))),
      InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig), batchDownsampler.rawDatasetRef, 0,
      queryFilters, AllChunkScan, "_metric_", colName = Option("sum"))
    val querySession = QuerySession(QueryContext(), queryConfig)
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
      .runToFuture(queryScheduler).futureValue.asInstanceOf[QueryResult]
    queryScheduler.shutdown()
    res.result.size shouldEqual 1
    res.result.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual
      List((74372982000L, 88.0), (74373042000L, 24.0))
    downsampleTSStore.shutdown()

  }

  it ("should fail when cardinality buster is not configured with any delete filters") {

    // This test case is important to ensure that a run with missing configuration will not do unintended deletes
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val settings2 = new DownsamplerSettings(conf)
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    val caught = intercept[SparkException] {
      cardBuster.run(sparkConf).close()
    }
    caught.getCause.asInstanceOf[ConfigException.Missing].getMessage
      .contains("No configuration setting found for key 'cardbuster'") shouldEqual true
  }

  it("should verify bulk part key records are all present before card busting") {

    val readKeys = (0 until numShards).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    readKeys.size shouldEqual 10012

    val readKeys2 = (0 until numShards).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    readKeys2.size shouldEqual 10013
  }

  it ("should be able to bust cardinality by time filter in downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString(
      s"""
         |filodb.cardbuster.delete-pk-filters.0._ns_ = bulk_ns
         |filodb.cardbuster.delete-pk-filters.0._ws_ = "b.*_ws"
         |filodb.cardbuster.delete-startTimeGTE = "${Instant.ofEpochMilli(0).toString}"
         |filodb.cardbuster.delete-startTimeLTE = "${Instant.ofEpochMilli(600).toString}"
         |filodb.cardbuster.delete-endTimeGTE = "${Instant.ofEpochMilli(500).toString}"
         |filodb.cardbuster.delete-endTimeLTE = "${Instant.ofEpochMilli(600).toString}"
         |""".stripMargin)

    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    sparkConf.set("spark.filodb.cardbuster.isSimulation", "false")
    cardBuster.run(sparkConf).close()
  }

  def pkMetricName(pkr: PartKeyRecord): String = {
    val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
    strPairs.find(p => p._1 == "_metric_").head._2
  }

  it("should verify bulk part key records are absent after card busting by time filter in downsample tables") {

    val readKeys = (0 until numShards).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    // downsample set should not have a few bulk metrics
    readKeys.size shouldEqual 9911

    val readKeys2 = (0 until numShards).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    // raw set should remain same since inDownsampleTables=true in
    readKeys2.size shouldEqual 10013
  }

  it ("should be able to bust cardinality in both raw and downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString( s"""
                                                          |filodb.cardbuster.delete-pk-filters = [
                                                          | {
                                                          |    _ns_ = "bulk_ns"
                                                          |    _ws_ = "bulk_ws"
                                                          | }
                                                          |]
                                                          |filodb.cardbuster.delete-startTimeGTE = "${Instant.ofEpochMilli(0).toString}"
                                                          |filodb.cardbuster.delete-startTimeLTE = "${Instant.ofEpochMilli(10000).toString}"
                                                          |filodb.cardbuster.delete-endTimeGTE = "${Instant.ofEpochMilli(500).toString}"
                                                          |filodb.cardbuster.delete-endTimeLTE = "${Instant.ofEpochMilli(10600).toString}"
                                                          |""".stripMargin)
    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)

    // first run for downsample tables
    sparkConf.set("spark.filodb.cardbuster.isSimulation", "false")
    cardBuster.run(sparkConf).close()

    // then run for raw tables
    sparkConf.set("spark.filodb.cardbuster.inDownsampleTables", "false")
    cardBuster.run(sparkConf).close()
  }

  it("should verify bulk part key records are absent after deletion in both raw and downsample tables") {

    val readKeys = (0 until numShards).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys shouldEqual (metricNames.toSet - untypedName)

    val readKeys2 = (0 until numShards).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runToFuture, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys2 shouldEqual metricNames.toSet
  }

  it ("should correctly calculate data-shape stats during bootstrap / refresh") {

    // The plan:
    //   (1) Write rows to the raw column store.
    //     (a) Populate a set of "expected" data-shape stats while iterating rows.
    //   (2) Downsample the chunks/index.
    //   (3) Simulate an index bootstrap and refresh.
    //   (4) Compare the index/bootstrap-generated stats with the stats from (1.a).
    val rawConf =
    """
      |data-shape-key: [_ws_,_ns_]
      |data-shape-key-publish-labels: [workspace, namespace]
      |data-shape-allow: [[foo_ws]]
      |data-shape-block: [[foo_ws,block_ns]]
      |enable-data-shape-bucket-count: true
      |""".stripMargin
    val conf = ConfigFactory.parseString(rawConf).withFallback(settings.downsamplerConfig)
    val dsConfig = DownsampleConfig(conf)
    val dsDataset = settings.downsampledDatasetRefs.last
    val shardNum = 0
    val ttl = Duration(10, TimeUnit.DAYS)
    val index = new PartKeyLuceneIndex(
      dsDataset, schemas.part, facetEnabledAllLabels = true,
      facetEnabledShardKeyLabels = true, shardNum, ttl.toMillis)
    val commonLabels = Map("_ws_" -> "foo_ws")
    val firstTimestampMs = 74372801000L
    val stepMs = 10000L

    // ====== clear all previous state ========================================

    beforeAll()  // truncate columnstores
    val offheapMem = new OffHeapMemory(
      Seq(Schemas.promCounter, Schemas.promHistogram), Map.empty, 100, rawDataStoreConfig)
    val shardInfo = TimeSeriesShardInfo(
      0, batchDownsampler.shardStats, offheapMem.bufferPools, offheapMem.nativeMemoryManager)

    // ====== configure the data to ingest / downsample =========================

    case class Series(metric: String,
                      labels: Map[String, String],
                      expected: Boolean,
                      schema: Schema,
                      bucketCount: Option[Int],
                      rows: Stream[Seq[Any]])

    val threeBucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val fourBucketScheme = CustomBuckets(Array(3d, 10d, 20, Double.PositiveInfinity))
    val fiveBucketScheme = CustomBuckets(Array(3d, 10d, 25d, 50d, Double.PositiveInfinity))

    val seriesSpecs = Seq(
      Series("three_buckets",
        Map("_ns_" -> "foo_ns", "a" -> "123", "b" -> "456", "c" -> "789"),
        expected = true,
        Schemas.promHistogram,
        bucketCount = Some(3),
        Stream(
          Seq(0d, 1d, LongHistogram(threeBucketScheme, Array(0L, 0, 1))),
          Seq(2d, 3d, LongHistogram(threeBucketScheme, Array(0L, 2, 3))),
          Seq(5d, 6d, LongHistogram(threeBucketScheme, Array(2L, 5, 6))),
        )),
      Series("five_buckets_delta",
        Map("_ns_" -> "foo_ns",
          "this_is_a_really_really_really_long_key" ->
          "wow such value incredible who could have guessed a value could be so long"),
        expected = true,
        Schemas.promHistogram,
        bucketCount = Some(5),
        Stream(
          Seq(0d, 1d, LongHistogram(fiveBucketScheme, Array(0L, 0, 1, 1, 1))),
          Seq(2d, 3d, LongHistogram(fiveBucketScheme, Array(0L, 2, 3, 3, 4))),
          Seq(5d, 6d, LongHistogram(fiveBucketScheme, Array(2L, 5, 6, 7, 8))),
        )),
      Series("just_a_counter",
        Map("_ns_" -> "foo_ns", "blue" -> "red", "green" -> "yellow"),
        expected = true,
        Schemas.promCounter,
        bucketCount = None,
        Stream(
          Seq(0d),Seq(2d),Seq(5d),
        )),
      Series("four_buckets_blocked",
        Map("_ns_" -> "block_ns", "a" -> "123", "b" -> "456", "c" -> "789"),
        expected = false,
        Schemas.promHistogram,
        bucketCount = Some(4),
        Stream(
          Seq(0d, 1d, LongHistogram(fourBucketScheme, Array(0L, 0, 1, 10))),
          Seq(2d, 3d, LongHistogram(fourBucketScheme, Array(0L, 2, 3, 12))),
          Seq(5d, 6d, LongHistogram(fourBucketScheme, Array(2L, 5, 6, 13))),
        )),
    )

    // Adds all recorded values to a list (for later validation).
    class MockHistogram extends metric.Histogram {
      val values = new mutable.ArrayBuffer[Long]()
      override def record(value: Long): Histogram = {
        values.synchronized {
          values.append(value)
        }
        this
      }
      override def withTags(tags: TagSet): Histogram = this
      override def record(value: Long, times: Long): Histogram = ???
      override def metric: Metric[Histogram, Settings.ForDistributionInstrument] = ???
      override def tags: TagSet = ???
      override def autoUpdate(consumer: Histogram => Unit, interval: time.Duration): Histogram = ???
    }

    // All mock histograms will effectively "log" each record() argument.
    // We'll validate these after the bootstrap/refresh are complete.
    class MockStats(dataset: DatasetRef, shardNum: Int) extends DownsampledTimeSeriesShardStats(dataset, shardNum) {
      override val dataShapeKeyLength = new MockHistogram
      override val dataShapeValueLength = new MockHistogram
      override val dataShapeLabelCount = new MockHistogram
      override val dataShapeMetricLength = new MockHistogram
      override val dataShapeTotalLength = new MockHistogram
      override val dataShapeBucketCount = new MockHistogram

      override def hashCode(): Int = 0  // compiler requires this

      // Make sure all lists contain the same values.
      override def equals(obj: Any): Boolean = {
        val other = obj.asInstanceOf[MockStats]
        dataShapeBucketCount.values.sorted == other.dataShapeBucketCount.values.sorted &&
          dataShapeMetricLength.values.sorted == other.dataShapeMetricLength.values.sorted &&
          dataShapeTotalLength.values.sorted == other.dataShapeTotalLength.values.sorted &&
          dataShapeKeyLength.values.sorted == other.dataShapeKeyLength.values.sorted &&
          dataShapeValueLength.values.sorted == other.dataShapeValueLength.values.sorted &&
          dataShapeLabelCount.values.sorted == other.dataShapeLabelCount.values.sorted
      }

      override def toString: String = {
        s"MockStats(key-len: ${dataShapeKeyLength.values.sorted}, " +
          s"val-len: ${dataShapeValueLength.values.sorted}, " +
          s"label-count: ${dataShapeLabelCount.values.sorted}, " +
          s"metric-length: ${dataShapeMetricLength.values.sorted}, " +
          s"total-length: ${dataShapeTotalLength.values.sorted}, " +
          s"bucket-count: ${dataShapeBucketCount.values.sorted})"
      }
    }

    // ======= begin the ingest / downsample process ==========

    // These stats will be populated as we write the configured rows to the ColumnStore.
    // The dataset/shard we use in each of these MockStats are not important.
    val expectedStats = new MockStats(dsDataset, shardNum)
    val nextTimestamp = new AtomicLong(firstTimestampMs)
    val nextPartId = new AtomicInteger(0)
    val metricKey = schemas.part.options.metricColumn
    seriesSpecs.foreach{ spec =>
      val labels = (spec.labels ++ commonLabels).map{ case (k, v) => k.utf8 -> v.utf8 }
      if (spec.expected) {
        // Also include the metric column as a label.
        for ((k, v) <- labels) {
          expectedStats.dataShapeKeyLength.record(k.length)
          expectedStats.dataShapeValueLength.record(v.length)
        }
        expectedStats.dataShapeKeyLength.record(metricKey.length)
        expectedStats.dataShapeValueLength.record(spec.metric.length)
        expectedStats.dataShapeLabelCount.record(labels.size + 1)
        expectedStats.dataShapeMetricLength.record(spec.metric.length)
        val totalLength = metricKey.length + spec.metric.length + labels.foldLeft(0){ case (length, (k, v)) =>
          length + k.length + v.length
        }
        expectedStats.dataShapeTotalLength.record(totalLength)
        if (spec.bucketCount.isDefined) {
          expectedStats.dataShapeBucketCount.record(spec.bucketCount.get)
        }
      }

      val rows = spec.rows.map{ raw =>
        Seq(nextTimestamp.getAndAdd(stepMs)) ++ raw ++ Seq(spec.metric, labels)
      }
      val startMs = rows.head(0).asInstanceOf[Long]
      val endMs = rows.last(0).asInstanceOf[Long]

      // Begin the ingestion process-- write records to the builder, then flush to the ColumnStore.
      val partKey = {
        val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
        partBuilder.partKeyFromObjects(spec.schema, spec.metric, labels)
      }
      val rawDataset = Dataset("prometheus", spec.schema)
      val partition = new TimeSeriesPartition(nextPartId.getAndIncrement(), spec.schema, partKey, shardInfo, 1)
      MachineMetricsData.records(rawDataset, rows).records.foreach { case (base, offset) =>
        val rr = new BinaryRecordRowReader(spec.schema.ingestionSchema, base, offset)
        partition.ingest(endMs, rr, offheapMem.blockMemFactory, createChunkAtFlushBoundary = false,
          flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
      }
      partition.switchBuffers(offheapMem.blockMemFactory, true)
      val chunks = partition.makeFlushChunks(offheapMem.blockMemFactory)
      rawColStore.write(rawDataset.ref, Observable.fromIteratorUnsafe(chunks)).futureValue
      val pk = PartKeyRecord(partition.partKeyBytes, startMs, endMs, shard)
      rawColStore.writePartKeys(rawDataset.ref, shard, Observable.now(pk), ttl.toSeconds, pkUpdateHour).futureValue
    }

    // sanity-check
    assert(expectedStats.dataShapeKeyLength.values.nonEmpty, "expectedStats should be nonempty")

    // downsample the chunks
    {
      val sparkConf = new SparkConf(loadDefaults = true)
      sparkConf.setMaster("local[2]")
      sparkConf.set("spark.filodb.downsampler.userTimeOverride", Instant.ofEpochMilli(nextTimestamp.get()).toString)
      val downsampler = new Downsampler(settings)
      downsampler.run(sparkConf).close()
    }

    // downsample the index
    {
      val sparkConf = new SparkConf(loadDefaults = true)
      sparkConf.setMaster("local[2]")
      sparkConf.set("spark.filodb.downsampler.index.timeInPeriodOverride", Instant.ofEpochMilli(nextTimestamp.get()).toString)
      val indexUpdater = new IndexJobDriver(settings, dsIndexJobSettings)
      indexUpdater.run(sparkConf).close()
    }

    // ======= do the refresh / bootstrap; compare stats with expected values ==========

    // FIXME: Each of these will fail on GitHub when parallelism != 1. When an abundance of logs are added in order to
    //   debug, the test will pass for all "parallelism" values.
    // The tests fail because the histogram bucket counts don't consistently appear in the MockStats. This is
    //   probably a race-condition that's exclusive to this test/MockStats setup.

    val refreshStats = new MockStats(dsDataset, shardNum)
    val rawDataset = Dataset("prometheus", Schemas.untyped)  // Exact schema does not matter here.
    val refresh = new DownsampleIndexBootstrapper(rawColStore, schemas, refreshStats, rawDataset.ref, dsConfig)
      .refreshWithDownsamplePartKeys(index, shardNum, rawDataset.ref, pkUpdateHour, pkUpdateHour, schemas, parallelism = 1)
    Await.result(refresh.runToFuture, 10.second)
    refreshStats shouldEqual expectedStats

    val bootstrapStats = new MockStats(dsDataset, shardNum)
    val bootstrap = new DownsampleIndexBootstrapper(downsampleColStore, schemas, bootstrapStats, dsDataset, dsConfig)
      .bootstrapIndexDownsample(index, shardNum, dsDataset, Long.MaxValue, parallelism = 1)
    Await.result(bootstrap.runToFuture, 10.second)
    bootstrapStats shouldEqual expectedStats
  }
}