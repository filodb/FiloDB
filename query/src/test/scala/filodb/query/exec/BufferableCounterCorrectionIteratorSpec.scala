package filodb.query.exec

import org.scalatest.{FunSpec, Matchers}

class BufferableCounterCorrectionIteratorSpec extends FunSpec with Matchers {

  it("should correct dips in counter") {
    val input = Seq(3, 5, 7, 13, 2, 34).map(d => new TransientRow(0, d))
    new BufferableCounterCorrectionIterator(input.iterator).map(_.value).toList shouldEqual Seq(3d, 5d, 7d, 13d, 15d,
      47d)
  }

  it("should correct multiple dips in counter") {
    val input = Seq(3, 5, 7, 13, 2, 34, 4, 6).map(d => new TransientRow(0, d))
    new BufferableCounterCorrectionIterator(input.iterator)
      .map(_.value).toList shouldEqual Seq(3d, 5d, 7d, 13d, 15d, 47d, 51d, 53d)
  }

  it("should not correct when no dips in counter") {
    val input = Seq(3, 5, 7, 13, 22, 34).map(d => new TransientRow(0, d))
    new BufferableCounterCorrectionIterator(input.iterator).map(_.value).toList shouldEqual Seq(3d, 5d, 7d, 13d, 22d,
      34d)
  }

  it("should be bufferable") {
    val input = Seq(3, 5, 7, 13, 2, 34, 4, 6).map(d => new TransientRow(0, d))

    val iter = new BufferableCounterCorrectionIterator(input.iterator).buffered

    iter.head.value shouldEqual 3d
    iter.next().value shouldEqual 3d

    iter.head.value shouldEqual 5d
    iter.next().value shouldEqual 5d

    iter.head.value shouldEqual 7d
    iter.next().value shouldEqual 7d

    iter.head.value shouldEqual 13d
    iter.next().value shouldEqual 13d

    iter.head.value shouldEqual 15d
    iter.next().value shouldEqual 15d

    iter.head.value shouldEqual 47d
    iter.next().value shouldEqual 47d

    iter.head.value shouldEqual 51d
    iter.next().value shouldEqual 51d

    iter.head.value shouldEqual 53d
    iter.next().value shouldEqual 53d
  }

  it("should handle empty set properly") {
    val input = Seq.empty[Double].map(d => new TransientRow(0, d))
    new BufferableCounterCorrectionIterator(input.iterator).hasNext shouldEqual false
  }
}

