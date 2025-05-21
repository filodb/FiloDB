package filodb.coordinator

import scala.util.Success
import scala.concurrent.duration._

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon
import kamon.tag.TagSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.core.query.{
  QueryStats,
  QueryWarnings,
  RangeVector,
  RangeVectorCursor,
  RangeVectorKey,
  ResultSchema,
  RvRange
}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{QueryResult, TsCardinalities}
import filodb.query.exec.TsCardExec._

class TenantIngestionMeteringSpec extends TestKit(ActorSystem("TenantIngestionMeteringSpec"))
  with AnyWordSpecLike with Matchers with BeforeAndAfterAll with Eventually {


  implicit override val patienceConfig = PatienceConfig(
    timeout = Span(30, Seconds),
    interval = Span(1, Seconds)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    Kamon.init()
  }

  override def afterAll(): Unit = {
    Kamon.stopModules()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val testConfig: Config = ConfigFactory.parseString("""
    filodb {
      metering-query-interval = 1 second
      cluster-type = "raw"
      partition = "test-partition"
      ingest-resolution-millis = 10000
      max-data-per-shard-query = 1048576
    }
    """)

  val settings = new FilodbSettings(testConfig)
  val testDataset = DatasetRef("test-dataset")
  val testWs = "test-ws"
  val testNs = "test-ns"

  val mockRowReader = new RowReader {
    override def notNull(columnNo: Int): Boolean = true
    override def getBoolean(columnNo: Int): Boolean = throw new UnsupportedOperationException
    override def getInt(columnNo: Int): Int = throw new UnsupportedOperationException
    override def getLong(columnNo: Int): Long = columnNo match {
      case 1 => 100L  // active
      case 2 => 150L  // shortTerm
      case 3 => 0L    // longTerm
      case _ => throw new IllegalArgumentException(s"Invalid index $columnNo")
    }
    override def getDouble(columnNo: Int): Double = throw new UnsupportedOperationException
    override def getFloat(columnNo: Int): Float = throw new UnsupportedOperationException
    override def getString(columnNo: Int): String = throw new UnsupportedOperationException
    override def getAny(columnNo: Int): Any = columnNo match {
      case 0 => ZeroCopyUTF8String(s"${testWs}${PREFIX_DELIM}${testNs}")
      case _ => throw new IllegalArgumentException(s"Invalid index $columnNo")
    }
    override def getBlobBase(columnNo: Int): Any = throw new UnsupportedOperationException
    override def getBlobOffset(columnNo: Int): Long = throw new UnsupportedOperationException
    override def getBlobNumBytes(columnNo: Int): Int = throw new UnsupportedOperationException
  }

  class MockCoordinator(probe: TestProbe) extends Actor {
    def receive: Receive = {
      case msg @ LogicalPlan2Query(dsRef, logicalPlan: TsCardinalities, qContext, _) =>
        probe.ref forward msg
        val rangeVector = new RangeVector {
          override def key: RangeVectorKey = new RangeVectorKey {
            override def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
            override def sourceShards: Seq[Int] = Seq.empty
            override def partIds: Seq[Int] = Seq.empty
            override def schemaNames: Seq[String] = Seq.empty
            override def keySize: Int = 0
          }
          override def rows: RangeVectorCursor = new RangeVectorCursor {
            private var hasNextValue = true
            override def hasNext: Boolean = hasNextValue
            override def next(): RowReader = {
              if (!hasNextValue) throw new NoSuchElementException
              hasNextValue = false
              mockRowReader
            }
            override def close(): Unit = {}
          }
          override def outputRange: Option[RvRange] = None
          override def numRows: Option[Int] = Some(1)
        }

        val queryResult = QueryResult(
          id = dsRef.dataset,
          resultSchema = ResultSchema.empty,
          result = Seq(rangeVector),
          queryStats = QueryStats(),
          warnings = QueryWarnings(),
          mayBePartial = false,
          partialResultReason = None
        )
        sender() ! Success(queryResult)
    }
  }

  def createMetering(probe: TestProbe): TenantIngestionMetering = {
    val dsIterProducer = () => Iterator(testDataset)
    val coordActor = system.actorOf(Props(new MockCoordinator(probe)))
    val coordActorProducer = () => coordActor
    TenantIngestionMetering(settings, dsIterProducer, coordActorProducer)
  }

  "TenantIngestionMetering" should {
    "publish metrics correctly for raw cluster type" in {
      val probe = TestProbe()
      val metering = createMetering(probe)

      // Trigger metric publishing
      metering.schedulePeriodicPublishJob()

      // Verify the query is sent with correct parameters
      probe.expectMsgPF(10.seconds) {
        case LogicalPlan2Query(dsRef, logicalPlan: TsCardinalities, qContext, _) =>
          dsRef shouldBe testDataset
          logicalPlan.shardKeyPrefix shouldBe Nil
          logicalPlan.numGroupByFields shouldBe 2
          logicalPlan.overrideClusterName shouldBe "raw"
          qContext.traceInfo should contain("filodb.partition" -> "test-partition")
      }

      // Wait and verify metrics were published with correct values
      val tags = Map(
        "metric_ws" -> testWs,
        "metric_ns" -> testNs,
        "dataset" -> testDataset.dataset,
        "cluster_type" -> "raw",
        "_ws_" -> testWs,
        "_ns_" -> testNs
      )

      eventually {
        // Verify active timeseries metric
        val activeGauge = Kamon.gauge("tsdb_metering_active_timeseries")
        val activeValue = activeGauge.withTags(TagSet.from(tags))
        println(s"Active timeseries - Expected: 100.0, Actual: $activeValue")
        activeValue shouldBe 100.0 +- 0.1

        // Verify total timeseries metric
        val totalGauge = Kamon.gauge("tsdb_metering_total_timeseries")
        val totalValue = totalGauge.withTags(TagSet.from(tags))
        println(s"Total timeseries - Expected: 150.0, Actual: $totalValue")
        totalValue shouldBe 150.0 +- 0.1

        // Verify retained timeseries metric
        val retainedGauge = Kamon.gauge("tsdb_metering_retained_timeseries")
        val retainedValue = retainedGauge.withTags(TagSet.from(tags))
        println(s"Retained timeseries - Expected: 100.0, Actual: $retainedValue")
        retainedValue shouldBe 100.0 +- 0.1

        // Verify samples ingested per minute (based on 10s resolution)
        val expectedSamplesPerMin = 100.0 * (60000.0 / 10000)  // 600.0
        val samplesGauge = Kamon.gauge("tsdb_metering_samples_ingested_per_min")
        val samplesValue = samplesGauge.withTags(TagSet.from(tags))
        println(s"Samples ingested per min - Expected: $expectedSamplesPerMin, Actual: $samplesValue")
        samplesValue shouldBe expectedSamplesPerMin +- 0.1

        // Verify query bytes scanned per minute
        val expectedBytesPerMin = 100.0 * 1048576.0  // Using raw bytes now
        val bytesGauge = Kamon.gauge("tsdb_metering_query_samples_scanned_per_min")
        val bytesValue = bytesGauge.withTags(TagSet.from(tags))
        println(s"Query bytes scanned per min - Expected: $expectedBytesPerMin, Actual: $bytesValue")
        bytesValue shouldBe expectedBytesPerMin +- 0.1

        // Print all tags being used
        println(s"Using tags: $tags")
      }
    }
  }
}
