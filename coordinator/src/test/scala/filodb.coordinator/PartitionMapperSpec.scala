package filodb.coordinator

import akka.actor.{Address, ActorRef}
import scala.util.Random

import org.scalatest.{FunSpec, Matchers}

class PartitionMapperSpec extends FunSpec with Matchers {
  val emptyRef = ActorRef.noSender
  val addr1 = Address("http", "test", "host1", 1001)
  val addr2 = Address("http", "test2", "host2", 1002)

  it("should be able to add one node at a time immutably") {
    val mapper = PartitionMapper.empty.addNode(addr1, emptyRef)
    mapper.isEmpty should be (false)
    mapper.numNodes should be (8)

    val mapper2 = mapper.addNode(addr2, emptyRef, numReplicas = 4)
    mapper2.isEmpty should be (false)
    mapper2.numNodes should be (12)
    mapper.numNodes should be (8)    // original mapper should be untouched
  }

  it("should be able to remove nodes") {
    val mapper = PartitionMapper.empty.addNode(addr1, emptyRef).addNode(addr2, emptyRef)
    mapper.numNodes should be (16)

    val mapper2 = mapper.removeNode(addr2)
    mapper2.numNodes should be (8)
    mapper.numNodes should be (16)

    // removing something not there should not affect
    mapper2.removeNode(addr2).numNodes should be (8)
  }

  it("should get an exception if try to lookup coordinator for empty mapper") {
    intercept[RuntimeException] {
      PartitionMapper.empty.lookupCoordinator(0)
    }
  }

  it("should get back coordRefs for different partition key hashes") {
    // Single node should return same ref for all kinds of hashes
    val mapper = PartitionMapper.empty.addNode(addr1, emptyRef)
    mapper.lookupCoordinator(0) should equal (emptyRef)
    mapper.lookupCoordinator(Int.MaxValue) should equal (emptyRef)
    mapper.lookupCoordinator(-1) should equal (emptyRef)
    mapper.lookupCoordinator(Int.MinValue) should equal (emptyRef)
    (0 to 10).foreach { i => mapper.lookupCoordinator(Random.nextInt) should equal (emptyRef) }
  }
}