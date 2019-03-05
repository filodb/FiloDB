package filodb.query.exec

import scala.annotation.tailrec

import org.scalatest.{FunSpec, Matchers}

// scalastyle:off null
class StitchRvsExecSpec extends FunSpec with Matchers {
  val error = 0.0000001d

  it ("should merge with two overlapping RVs correctly") {
    val rvs = Seq (
      Seq(  (10L, 3d),
            (20L, 3d),
            (30L, 3d),
            (40L, 3d),
            (50L, 3d)
      ),
      Seq(  (30L, 4d),
            (50L, 4d),
            (60L, 3d),
            (70L, 3d),
            (80L, 3d),
            (90L, 3d),
            (100L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
            (20L, 3d),
            (30L, Double.NaN),
            (40L, 3d),
            (50L, Double.NaN),
            (60L, 3d),
            (70L, 3d),
            (80L, 3d),
            (90L, 3d),
            (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  it ("should merge one RV correctly") {
    val input =       Seq(  (10L, 3d),
      (20L, 3d),
      (30L, Double.NaN),
      (40L, 3d),
      (50L, Double.NaN),
      (60L, 3d),
      (70L, 3d),
      (80L, 3d),
      (90L, 3d),
      (100L, 3d)
    )
    mergeAndValidate(Seq(input), input)
  }
  it ("should merge with three overlapping RVs correctly") {
    val rvs = Seq (
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      ),
      Seq(  (30L, 4d),
        (50L, 4d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq(  (30L, 4d),
        (55L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, Double.NaN),
        (40L, 3d),
        (50L, Double.NaN),
        (55L, 3d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }


  it ("should merge with no overlap correctly") {
    val rvs = Seq (
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d)
      )
    )
    val expected =
      Seq(  (10L, 3d),
        (20L, 3d),
        (30L, 3d),
        (40L, 3d),
        (50L, 3d),
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }


  it ("should merge with one empty rv correctly") {
    val rvs = Seq (
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      ),
      Seq()
    )
    val expected =
      Seq(
        (60L, 3d),
        (70L, 3d),
        (80L, 3d),
        (90L, 3d),
        (100L, 3d)
      )
    mergeAndValidate(rvs, expected)
  }

  def mergeAndValidate(rvs: Seq[Seq[(Long, Double)]], expected: Seq[(Long, Double)]): Unit = {
    val inputSeq = rvs.map { rows => rows.iterator.map(r => new TransientRow(r._1, r._2)) }
    val result = StitchRvsExec.merge(inputSeq).map(r => (r.getLong(0), r.getDouble(1)))
    compareIter(result, expected.toIterator)
  }

  @tailrec
  final private def compareIter(it1: Iterator[(Long, Double)], it2: Iterator[(Long, Double)]) : Unit = {
    (it1.hasNext, it2.hasNext) match{
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        v1._1 shouldEqual v2._1
        if (v1._2.isNaN) v2._2.isNaN shouldEqual true
        else Math.abs(v1._2-v2._2) should be < error
        compareIter(it1, it2)
      case (false, false) => Unit
      case _ => fail("Unequal lengths")
    }
  }

}

