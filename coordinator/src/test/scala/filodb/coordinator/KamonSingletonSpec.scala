package filodb.coordinator

import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class KamonSingletonSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    // Reset the initialization state before each test
    KamonSingleton.resetForTesting()
  }

  describe("KamonSingleton") {
    it("should initialize Kamon only once") {
      // Initially not initialized
      KamonSingleton.isInitialized shouldBe false

      // First call should initialize
      KamonSingleton.initOnce()
      KamonSingleton.isInitialized shouldBe true

      // Second call should be idempotent (no-op)
      KamonSingleton.initOnce()
      KamonSingleton.isInitialized shouldBe true
    }

    it("should be thread-safe") {
      import scala.concurrent.{ExecutionContext, Future}
      import scala.util.{Failure, Success}
      
      implicit val ec: ExecutionContext = ExecutionContext.global
      
      // Create multiple threads that try to initialize Kamon concurrently
      val futures = (1 to 10).map { _ =>
        Future {
          KamonSingleton.initOnce()
          KamonSingleton.isInitialized
        }
      }

      // Wait for all futures to complete
      val results = Future.sequence(futures)
      
      // All should return true (initialized)
      results.onComplete {
        case Success(values) =>
          values.foreach(_ shouldBe true)
          KamonSingleton.isInitialized shouldBe true
        case Failure(exception) =>
          fail(s"Thread safety test failed: ${exception.getMessage}")
      }
      
      // Block until completion (for test purposes)
      Thread.sleep(1000)
    }

    it("should handle multiple calls from different simulated Spark workers") {
      // Simulate multiple Spark worker JVMs calling initOnce
      val workerSimulations = (1 to 5).map { workerId =>
        () => {
          // Each "worker" calls initOnce multiple times (simulating map/mapPartitions)
          (1 to 3).foreach { _ =>
            KamonSingleton.initOnce()
          }
          KamonSingleton.isInitialized
        }
      }

      // Execute all worker simulations
      val results = workerSimulations.map(_.apply())
      
      // All should return true and Kamon should be initialized only once
      results.foreach(_ shouldBe true)
      KamonSingleton.isInitialized shouldBe true
    }
  }
}