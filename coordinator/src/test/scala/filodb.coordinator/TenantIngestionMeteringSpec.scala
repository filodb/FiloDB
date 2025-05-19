package filodb.coordinator


import scala.util.Success
import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.core.query.{QueryStats, RangeVector, RangeVectorCursor, RangeVectorKey, ResultSchema, RvRange}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.exec.TsCardExec
import filodb.query.{QueryError, QueryResult}
import org.scalacheck.Prop.False


class TenantIngestionMeteringSpec extends TestKit(ActorSystem("TenantIngestionMeteringSpec"))
  with AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val testConfig: Config = ConfigFactory.parseString("""
    metering-query-interval = 1 second
    cluster-type = "raw"
    partition = "test-partition"
    ingest-resolution-millis = 10000
    max-data-per-shard-query = 1MB
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
      case 3 => 200L  // longTerm
      case _ => throw new IllegalArgumentException(s"Invalid index $columnNo")
    }

    override def getDouble(columnNo: Int): Double = throw new UnsupportedOperationException

    override def getFloat(columnNo: Int): Float = throw new UnsupportedOperationException

    override def getString(columnNo: Int): String = throw new UnsupportedOperationException

    override def getAny(columnNo: Int): Any = columnNo match {
      case 0 => {
        val groupStr = s"$testWs${TsCardExec.PREFIX_DELIM}$testNs"
        ZeroCopyUTF8String(groupStr)
      }
      case _ => throw new IllegalArgumentException(s"Invalid index $columnNo")
    }

    override def getBlobBase(columnNo: Int): Any = throw new UnsupportedOperationException

    override def getBlobOffset(columnNo: Int): Long = throw new UnsupportedOperationException

    override def getBlobNumBytes(columnNo: Int): Int = throw new UnsupportedOperationException
  }

  // Mock actor that simulates a coordinator
  class MockCoordinator extends Actor {
    def receive: Receive = {
      case LogicalPlan2Query(dsRef, _, _, _) =>
        // Create a mock RowReader that will provide the test data

        // Create a RangeVector that wraps the RowData
        val rangeVector = new RangeVector {
          override def key: RangeVectorKey = new RangeVectorKey {
            override def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
            override def sourceShards: Seq[Int] = Nil 
            override def partIds: Seq[Int] = Nil
            override def schemaNames: Seq[String] = Nil
            override def keySize: Int = 0
            override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
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

        val queryStats = QueryStats()
        val resultSchema = ResultSchema(columns = Seq.empty, numRowKeyColumns = 0)
        sender() ! Success(QueryResult(
          id = dsRef.dataset,
          resultSchema = resultSchema,
          result = Seq(rangeVector),
          queryStats = queryStats,
          mayBePartial = false,
          partialResultReason = None
        ))
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
      
      // Mock Kamon gauge updates
      val activeGauge = Kamon.gauge("tsdb_metering_active_timeseries")
      val totalGauge = Kamon.gauge("tsdb_metering_total_timeseries")
      val samplesGauge = Kamon.gauge("tsdb_metering_samples_ingested_per_min")
      val bytesGauge = Kamon.gauge("tsdb_metering_query_samples_scanned_per_min")
      val retainedGauge = Kamon.gauge("tsdb_metering_retained_timeseries")

      // Trigger metric publishing
      metering.schedulePeriodicPublishJob()

      // Wait for metrics to be published
      Thread.sleep(2000)

      // Verify gauge values
      activeGauge should not be null
      totalGauge should not be null
      samplesGauge should not be null
      bytesGauge should not be null
      retainedGauge should not be null
    }
  }
} 
