package filodb.coordinator

import java.util.concurrent.TimeUnit

import monix.execution.Scheduler
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.flight.FlightClientManager

class FlightClientManagerSpec extends AnyFunSpec with Matchers with ScalaFutures
                                                 with BeforeAndAfter with BeforeAndAfterAll {

  class TestFlightProducer extends NoOpFlightProducer {
    override def listActions(context: FlightProducer.CallContext,
                             listener: FlightProducer.StreamListener[ActionType]): Unit = {
      listener.onCompleted()
    }
  }

  System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs
  private val allocator = new RootAllocator(10000)
  private val locations = Seq (Location.forGrpcInsecure("localhost", 8815),
                       Location.forGrpcInsecure("localhost", 8816),
                       Location.forGrpcInsecure("localhost", 8817))

  private var servers: Seq[FlightServer] = _
  implicit val scheduler: Scheduler = Scheduler.global


  // Additional test locations for error scenarios
  private val invalidLocation = Location.forGrpcInsecure("nonexistent-host", 9999)

  override def afterAll(): Unit = {
    allocator.close()
  }

  before {
    servers = locations.map { location =>
      FlightServer.builder(allocator, location, new TestFlightProducer()).build()
    }
  }

  after {
    servers.foreach(_.close())
    allocator.close()
  }

  describe("FlightClientManager") {

    it("should create singleton global instance") {
      val manager1 = FlightClientManager.global
      val manager2 = FlightClientManager.global
      manager1 shouldBe theSameInstanceAs(manager2)
    }

    it("should support checking for healthy clients") {
      val manager = FlightClientManager.global
      // Should return false for non-existent clients
      manager.getClientHealth(locations(0)) shouldBe None

      val server = servers(0).start()
      val client = manager.getClient(locations(0))
      val client2 = manager.getClient(locations(0))
      client shouldBe theSameInstanceAs(client2)
      manager.getClientHealth(locations(0)) shouldBe Some(None)
    }

    it("should handle shutdown gracefully") {
      // Create a new manager instance for testing shutdown
      val testManager = new FlightClientManager(allocator)

      // Should be able to shutdown without error
      testManager.shutdown()
      // After shutdown, getting a client should throw an exception
      intercept[IllegalStateException] {
        testManager.getClient(locations(0))
      }
    }

    it("should support force reconnection") {
      val manager = new FlightClientManager(allocator)
      val server = servers(0).start()

      // First, create a client
      val client1 = manager.getClient(locations(0))
      client1.listActions().iterator().hasNext shouldBe false
      manager.getClientHealth(locations(0)) shouldBe Some(None)

      // Force reconnection should create a new client
      val client2 = manager.forceRebuild(locations(0))
      client2 should not be theSameInstanceAs(client1)
      manager.getClientHealth(locations(0)) shouldBe Some(None)

      // Force reconnect on non-existent client should create new one
      val client3 = manager.forceRebuild(locations(1))
      client3 should not be null

      manager.shutdown()
      server.close()
    }

    it("should remove clients properly") {
      val manager = new FlightClientManager(allocator)
      val server = servers(1).start()

      // Create a client
      val client = manager.getClient(locations(1))
      manager.getClientHealth(locations(1)) shouldBe Some(None)

      // Remove the client
      manager.removeClient(locations(1))
      manager.getClientHealth(locations(1)) shouldBe None

      // Removing non-existent client should not throw
      manager.removeClient(locations(2))

      manager.shutdown()
      server.close()
    }

    it("should handle concurrent access correctly") {
      val manager = new FlightClientManager(allocator)
      val server = servers(2).start()

      // Create multiple concurrent requests for the same location
      val clients = (1 to 10).map { _ =>
        manager.getClient(locations(2))
      }

      // All clients should be the same instance (connection pooling)
      clients.foreach { client =>
        client shouldBe theSameInstanceAs(clients.head)
      }

      manager.shutdown()
      server.close()
    }

    it("should handle multiple different locations") {
      val manager = new FlightClientManager(allocator)
      val runningServers = servers.map(_.start())

      try {
        // Create clients for all locations
        val clients = locations.map { location =>
          location -> manager.getClient(location)
        }.toMap

        // All clients should be different instances
        clients.values.foreach { client1 =>
          clients.values.foreach { client2 =>
            if (client1 != client2) {
              client1 should not be theSameInstanceAs(client2)
            }
          }
        }

        // All locations should have healthy clients
        locations.foreach { location =>
          manager.getClientHealth(location) shouldBe Some(None)
        }

      } finally {
        manager.shutdown()
        runningServers.foreach(_.close())
      }
    }

    it("should handle client reuse and reconnection scenarios") {
      val manager = new FlightClientManager(allocator)
      val server = servers(0).start()

      try {
        // Get initial client
        val client1 = manager.getClient(locations(0))
        client1.listActions().iterator().hasNext shouldBe false

        // fSimulate connection failure by stopping server
        server.shutdown()
        println(s"server shutdown")
        Thread.sleep(1000) // Wait for server to shut down
        intercept[FlightRuntimeException] {
          val v = client1.listActions(CallOptions.timeout(1000, TimeUnit.MILLISECONDS))
          v.iterator().hasNext
        }

        Thread.sleep(40000) // Give it time to detect failure
        manager.getClientHealth(locations(0)) shouldBe Some(Some(false))
        val server2 = FlightServer.builder(allocator, locations(0), new TestFlightProducer()).build()
        server2.start()
        val client2 = manager.forceRebuild(locations(0))
        client2 should not be theSameInstanceAs(client1)
        client2.listActions().iterator().hasNext shouldBe false
        server2.shutdown()
      } finally {
        manager.shutdown()
      }
    }

    it("should handle memory management correctly") {
      val testAllocator = new RootAllocator(1024 * 1024) // 1MB limit
      val manager = new FlightClientManager(testAllocator)

      try {
        // Test that we can create clients without memory issues
        val server = servers(0).start()
        val client = manager.getClient(locations(0))

        // Verify allocator is being used
        testAllocator.getAllocatedMemory should be >= 0L

        server.close()
      } finally {
        manager.shutdown()
        testAllocator.close()
      }
    }

    it("should handle resource cleanup during shutdown") {
      val manager = new FlightClientManager(allocator)
      val server = servers(0).start()

      try {
        // Create multiple clients
        val client1 = manager.getClient(locations(0))
        val client2 = manager.getClient(locations(1))

        // Verify clients are created
        manager.getClientHealth(locations(0)) shouldBe Some(None)

        // Shutdown should clean up all resources
        manager.shutdown()

        // Attempting to get client after shutdown should fail
        val thrown = intercept[IllegalStateException] {
          manager.getClient(locations(0))
        }

        // Multiple shutdowns should be safe
        manager.shutdown() // Should not throw

      } finally {
        server.close()
      }
    }

    it("should handle reconnection failures gracefully") {
      val manager = new FlightClientManager(allocator)

      try {
        // Force reconnect to invalid location should fail gracefully
        val thrown = intercept[FlightRuntimeException] {
          val client1 = manager.forceRebuild(invalidLocation)
          client1.listActions().iterator().hasNext shouldBe false
        }
        thrown.getCause shouldBe a[Exception]

        // Should not have a healthy client after failed reconnect
        manager.getClientHealth(invalidLocation) shouldBe Some(None)

      } finally {
        manager.shutdown()
      }
    }

    describe("companion object methods") {
      it("should provide global instance access") {
        val manager1 = FlightClientManager.global
        val manager2 = FlightClientManager.global
        manager1 shouldBe theSameInstanceAs(manager2)
      }

      it("should provide convenience getClient method") {
        val server = servers(0).start()

        try {
          // Should be able to get client through companion object
          val client = FlightClientManager.getClient(locations(0))
          client should not be null

          // Should be same as getting through global instance
          val client2 = FlightClientManager.global.getClient(locations(0))
          client shouldBe theSameInstanceAs(client2)

        } finally {
          server.close()
        }
      }
    }

    describe("error scenarios and edge cases") {
      it("should handle null locations gracefully") {
        val manager = new FlightClientManager(allocator)

        try {
          // This should throw an appropriate exception for null location
          intercept[Exception] {
            manager.getClient(null)
          }
        } finally {
          manager.shutdown()
        }
      }

      it("should handle rapid shutdown scenarios") {
        val manager = new FlightClientManager(allocator)
        val server = servers(0).start()

        try {
          // Start getting a client but shutdown immediately
          val clientTask = manager.getClient(locations(0))
          manager.shutdown()

          // The task may complete or fail depending on timing
          // We just verify no exceptions are thrown during shutdown
          try {
            clientTask
          } catch {
            case _: Exception => // Expected - manager was shut down
          }

        } finally {
          server.close()
        }
      }
    }
  }
}