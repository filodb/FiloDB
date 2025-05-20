package filodb.coordinator

import scala.util.Success

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon
import kamon.tag.TagSet
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.core.query.{QueryStats, QueryWarnings, RangeVector, RangeVectorCursor, RangeVectorKey, ResultSchema}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{QueryResult, TsCardinalities}
import filodb.query.exec.TsCardExec._

class TenantIngestionMeteringSpec extends TestKit(ActorSystem("TenantIngestionMeteringSpec"))
  with AnyWordSpecLike with Matchers with BeforeAndAfterAll with Eventually {

  implicit override val patienceConfig = PatienceConfig(
    timeout = Span(5, Seconds),
    interval = Span(100, Millis)
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

  class MockCoordinator extends Actor {
    def receive: Receive = {
      case LogicalPlan2Query(dsRef, logicalPlan: TsCardinalities, qContext, _) =>
        val rangeVector = new RangeVector {
          override def key: RangeVectorKey = new RangeVectorKey {
            def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
            def sourceShards: Seq[Int] = Seq.empty 
            def partIds: Seq[Int] = Seq.empty
            def schemaNames: Seq[String] = Seq.empty
            def keySize: Int = 0
          }
          override def rows: RangeVectorCursor = new RangeVectorCursor { self =>
            override def hasNext: Boolean = ???
            override def next(): RowReader = {
              mockRowReader
            }
            override def close(): Unit = {}
            override def mapRow(f: RowReader => RowReader): RangeVectorCursor = new RangeVectorCursor {
              def hasNext = self.hasNext
              def next() = f(self.next())
              def close(): Unit = self.close()
            }
          }
          override def outputRange: Option[RvRange] = None
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

  def createMetering: TenantIngestionMetering = {
    val dsIterProducer = () => Iterator(testDataset)
    val coordActor = system.actorOf(Props[MockCoordinator])
    val coordActorProducer = () => coordActor
    TenantIngestionMetering(settings, dsIterProducer, coordActorProducer)
  }

  "TenantIngestionMetering" should {
    "publish metrics correctly for raw cluster type" in {
      val metering = createMetering

      // Trigger metric publishing
      metering.schedulePeriodicPublishJob()

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
        Kamon.gauge("tsdb_metering_active_timeseries")
          .withTags(TagSet.from(tags))
          .shouldEqual(100.0 +- 0.1)

        // Verify total timeseries metric
        Kamon.gauge("tsdb_metering_total_timeseries")
          .withTags(TagSet.from(tags))
          .shouldEqual(150.0 +- 0.1)

        // Verify retained timeseries metric
        Kamon.gauge("tsdb_metering_retained_timeseries")
          .withTags(TagSet.from(tags))
          .shouldEqual(100.0 +- 0.1)

        // Verify samples ingested per minute (based on 10s resolution)
        val expectedSamplesPerMin = 100.0 * (60000.0 / 10000)  // 600.0
        Kamon.gauge("tsdb_metering_samples_ingested_per_min")
          .withTags(TagSet.from(tags))
          .shouldEqual(expectedSamplesPerMin +- 0.1)

        // Verify query bytes scanned per minute
        val expectedBytesPerMin = 100.0 * (1048576.0 / 1000000)  // ~104.86
        Kamon.gauge("tsdb_metering_query_samples_scanned_per_min")
          .withTags(TagSet.from(tags))
          .shouldEqual(expectedBytesPerMin +- 0.1)
      }
    }
  }
}
