package filodb.query.exec.rangefn

import scala.util.Random

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query._
import filodb.query.exec.{StaticFuncArgs, TimeFuncArgs}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BinaryOperatorSpec extends AnyFunSpec with Matchers with ScalaFutures {

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))
  val sampleBase: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(3L, 3239.3423d),
        new TransientRow(4L, 94935.1523d)).iterator
    })
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val rand = new Random()
  val error = 0.00000001d
  val scalar = 5.0

  it("should work with Binary Operator mapper") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array.fill(100)(new RangeVector {
      val data: Stream[TransientRow] = Stream.from(0).map { n =>
        new TransientRow(n.toLong, rand.nextDouble())
      }.take(20)

      override def key: RangeVectorKey = ignoreKey

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = data.iterator
    })
    fireBinaryOperatorTests(samples, scalar)
    fireComparatorOperatorTests(samples, scalar)

  }

  it ("should handle NaN") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, Double.NaN),
          new TransientRow(2L, 5.6d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 4.6d),
          new TransientRow(2L, 4.4d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 0d),
          new TransientRow(2L, 5.4d)).iterator
      }
    )
    fireBinaryOperatorTests(samples, Double.NaN)
    fireComparatorOperatorTests(samples, Double.NaN)

  }

  it ("should handle special cases") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 2.0d/0d),
          new TransientRow(2L, 4.5d),
          new TransientRow(2L, 0d),
          new TransientRow(2L, -2.1d),
          new TransientRow(2L, 5.9d),
          new TransientRow(2L, Double.NaN),
          new TransientRow(2L, 3.3d)).iterator
      }
    )
    fireBinaryOperatorTests(samples, scalar)
    fireComparatorOperatorTests(samples, scalar)
  }

  private def fireBinaryOperatorTests(samples: Array[RangeVector], scalar: Double): Unit = {

    // Subtraction - prefix
    val expectedSub1 = samples.map(_.rows.map(v => scalar - v.getDouble(1)))
    applyBinaryOperationAndAssertResult(samples, expectedSub1, BinaryOperator.SUB, scalar, true)

    // Subtraction - suffix
    val expectedSub2 = samples.map(_.rows.map(v => v.getDouble(1) - scalar))
    applyBinaryOperationAndAssertResult(samples, expectedSub2, BinaryOperator.SUB, scalar, false)

    // Addition - prefix
    val expectedAdd1 = samples.map(_.rows.map(v => scalar + v.getDouble(1)))
    applyBinaryOperationAndAssertResult(samples, expectedAdd1, BinaryOperator.ADD, scalar, true)

    // Addition - suffix
    val expectedAdd2 = samples.map(_.rows.map(v => v.getDouble(1) + scalar))
    applyBinaryOperationAndAssertResult(samples, expectedAdd2, BinaryOperator.ADD, scalar, false)

    // Multiply - prefix
    val expectedMul1 = samples.map(_.rows.map(v => scalar * v.getDouble(1)))
    applyBinaryOperationAndAssertResult(samples, expectedMul1, BinaryOperator.MUL, scalar, true)

    // Multiply - suffix
    val expectedMul2 = samples.map(_.rows.map(v => v.getDouble(1) * scalar))
    applyBinaryOperationAndAssertResult(samples, expectedMul2, BinaryOperator.MUL, scalar, false)

    // Modulo - prefix
    val expectedMod1 = samples.map(_.rows.map(v => scalar % v.getDouble(1)))
    applyBinaryOperationAndAssertResult(samples, expectedMod1, BinaryOperator.MOD, scalar, true)

    // Modulo - suffix
    val expectedMod2 = samples.map(_.rows.map(v => v.getDouble(1) % scalar))
    applyBinaryOperationAndAssertResult(samples, expectedMod2, BinaryOperator.MOD, scalar, false)

    // Division - prefix
    val expectedDiv1 = samples.map(_.rows.map(v => scalar / v.getDouble(1)))
    applyBinaryOperationAndAssertResult(samples, expectedDiv1, BinaryOperator.DIV, scalar, true)

    // Division - suffix
    val expectedDiv2 = samples.map(_.rows.map(v => v.getDouble(1) / scalar))
    applyBinaryOperationAndAssertResult(samples, expectedDiv2, BinaryOperator.DIV, scalar, false)

    // power - prefix
    val expectedPow1 = samples.map(_.rows.map(v => math.pow(scalar, v.getDouble(1))))
    applyBinaryOperationAndAssertResult(samples, expectedPow1, BinaryOperator.POW, scalar, true)

    // power - suffix
    val expectedPow2 = samples.map(_.rows.map(v => math.pow(v.getDouble(1), scalar)))
    applyBinaryOperationAndAssertResult(samples, expectedPow2, BinaryOperator.POW, scalar, false)

  }

  //scalastyle:off method.length
  //scalastyle:off cyclomatic.complexity
  private def fireComparatorOperatorTests(samples: Array[RangeVector], scalar: Double): Unit = {

    // GTE - prefix
    val expectedGTE = samples.map(_.rows.map(v => if (scalar >= v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedGTE, BinaryOperator.GTE, scalar, true)

    // GTR - prefix
    val expectedGTR = samples.map(_.rows.map(v => if (scalar > v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedGTR, BinaryOperator.GTR, scalar, true)

    // LTE - prefix
    val expectedLTE = samples.map(_.rows.map(v => if (scalar <= v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedLTE, BinaryOperator.LTE, scalar, true)

    // LTR - prefix
    val expectedLTR = samples.map(_.rows.map(v => if (scalar < v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedLTR, BinaryOperator.LSS, scalar, true)

    // EQL - prefix
    val expectedEQL = samples.map(_.rows.map(v => if (scalar == v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedEQL, BinaryOperator.EQL, scalar, true)

    // NEQ - prefix
    val expectedNEQ = samples.map(_.rows.map(v => if (scalar != v.getDouble(1)) scalar else Double.NaN))
    applyBinaryOperationAndAssertResult(samples, expectedNEQ, BinaryOperator.NEQ, scalar, true)

    // GTE_BOOL - prefix
    val expectedGTE_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar >= v.getDouble(1)) 1.0 else 0.0
    })
    applyBinaryOperationAndAssertResult(samples, expectedGTE_BOOL, BinaryOperator.GTE_BOOL, scalar, true)

    // GTR_BOOL - prefix
    val expectedGTR_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar > v.getDouble(1)) 1.0 else 0.0
    })
    applyBinaryOperationAndAssertResult(samples, expectedGTR_BOOL, BinaryOperator.GTR_BOOL, scalar, true)

    // LTE_BOOL - prefix
    val expectedLTE_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar <= v.getDouble(1)) 1.0 else 0.0
    })

    // LTR_BOOL - prefix
    val expectedLTR_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar < v.getDouble(1)) 1.0 else 0.0
    })
    applyBinaryOperationAndAssertResult(samples, expectedLTR_BOOL, BinaryOperator.LSS_BOOL, scalar, true)

    // EQL_BOOL - prefix
    val expectedEQL_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar == v.getDouble(1)) 1.0 else 0.0
    })
    applyBinaryOperationAndAssertResult(samples, expectedEQL_BOOL, BinaryOperator.EQL_BOOL, scalar, true)

    // NEQ_BOOL - prefix
    val expectedNEQ_BOOL = samples.map(_.rows.map { v =>
      if (scalar.isNaN || v.getDouble(1).isNaN) Double.NaN
      else if (scalar != v.getDouble(1)) 1.0 else 0.0
    })
    applyBinaryOperationAndAssertResult(samples, expectedNEQ_BOOL, BinaryOperator.NEQ_BOOL, scalar, true)
  }
  //scalastyle:on method.length
  //scalastyle:on cyclomatic.complexity

  it ("should fail with wrong calculation") {
    // ceil
    val expectedVal = sampleBase.map(_.rows.map(v => scala.math.floor(v.getDouble(1))))
    val binaryOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, true, Seq(StaticFuncArgs(scalar, RangeParams(0,0,0))))
    val resultObs = binaryOpMapper(Observable.fromIterable(sampleBase), querySession, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 should not equal val2
        }
      }
    }
  }

  it ("should work when scalar is time()") {
    // ceil
    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 1),
          new TransientRow(2L, 2)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 1),
          new TransientRow(2L, 2)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 1),
          new TransientRow(2L, 2)).iterator
      }
    )
    val expectedVal = samples.map(_.rows.map(v => v.getDouble(1) * 2))
    val binaryOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, true, Seq(TimeFuncArgs(RangeParams(1,1,4))))
    val resultObs = binaryOpMapper(Observable.fromIterable(samples), querySession, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    result.foreach(x=> println(x.toList))
    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it ("should work with Func Args") {
    val samples: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey
        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 15.004124836249305),
          new TransientRow(2L, 2)).iterator
      }
    )
    val expectedVal = samples.map(_.rows.map(v => scala.math.floor(v.getDouble(1))))
    val binaryOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, true, Seq(StaticFuncArgs(1571267260, RangeParams(0,0,0))))
    val resultObs = binaryOpMapper(Observable.fromIterable(samples), querySession, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 should not equal val2
        }
      }
    }
  }

  private def applyBinaryOperationAndAssertResult(samples: Array[RangeVector], expectedVal: Array[Iterator[Double]],
                                                  binOp: BinaryOperator, scalar: Double, scalarOnLhs: Boolean): Unit = {
    val scalarOpMapper = exec.ScalarOperationMapper(binOp, scalarOnLhs, Seq(StaticFuncArgs(scalar, RangeParams(0,0,0))))
    val resultObs = scalarOpMapper(Observable.fromIterable(samples), querySession, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    expectedVal.zip(result).foreach {
      case (ex, res) =>  {
        ex.zip(res).foreach {
          case (val1, val2) =>
            if (val1.isInfinity) val2.isInfinity shouldEqual true
            else if (val1.isNaN) val2.isNaN shouldEqual true
            else val1 shouldEqual val2
        }
      }
    }
  }

}