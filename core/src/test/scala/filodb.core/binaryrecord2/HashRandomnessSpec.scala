package filodb.core.binaryrecord2

import org.scalatest.{FunSpec, Matchers}

/**
 * A test to ensure that the hashing algorithm used in RecordBuilder for maps is random enough
 */
class HashRandomnessSpec extends FunSpec with Matchers {

  val NumPairs = 10000

  // val apps = Seq("prometheus", "cassandra", "kafka", "filodb")
  def genPairs(i: Int): Map[String, String] = {
    // val app = apps(i % apps.length)
    Map("__name__" -> s"Counter${i % 100}", "job" -> s"App-$i")
  }

  it("RecordBuilder.sortAndComputeHashes/combineHashes should be random enough") {
    val allPairs = (0 until NumPairs).map(genPairs)

    val shardHashes = allPairs.map { case pairs =>
      RecordBuilder.shardKeyHash(Seq(pairs("job")), pairs("__name__"))
    }

    // println(s"shardHashes=${shardHashes.take(100)}")

    // Now, take the upper byte of shardHashes and gather histogram
    val histo = new Array[Int](256)
    shardHashes.foreach { hash =>
      histo((hash >> 24) & 0x0ff) += 1
    }
    (0 until 16).foreach { step =>
      println((step * 16 to (step * 16 + 15)).map(histo).mkString(", "))
    }

    val topBucket = histo.max
    val bottomBucket = histo.min
    println(s"Top bucket has $topBucket entries, bottom has $bottomBucket entries")

    (topBucket.toDouble / bottomBucket) should be < (2.5)
  }
}
