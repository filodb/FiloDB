package filodb.prometheus.ast

import org.scalatest.{FunSpec, Matchers}

import filodb.prometheus.parse.Parser

class ScalarExpressionSpec extends FunSpec with Matchers with Scalars {

  it("should parse and evaluate Scalar Arithmetic Expression") {
    val query = "1 + 2"
    val expression = Parser.parseQuery(query)
    expression.isInstanceOf[ArithmeticExpression] shouldEqual(true)
    expression.asInstanceOf[ArithmeticExpression].toScalar shouldEqual(3)
  }

  it("should parse and evaluate Scalar Boolean Expression") {
    val query = "1 == bool(2)"
    val expression = Parser.parseQuery(query)
    expression.isInstanceOf[BooleanExpression] shouldEqual(true)
    expression.asInstanceOf[BooleanExpression].toScalar shouldEqual(0)
  }

  it("should parse and evaluate Scalar Boolean Expression with <") {
    val query = "1 < bool(2)"
    val expression = Parser.parseQuery(query)
    expression.isInstanceOf[BooleanExpression] shouldEqual(true)
    expression.asInstanceOf[BooleanExpression].toScalar shouldEqual(1)
  }

  it("should validate invalidate queries") {
   the[IllegalArgumentException] thrownBy {
     Parser.parseQuery("1 < 2")
   } should have message "requirement failed: comparisons between scalars must use BOOL modifier"

   the[IllegalArgumentException] thrownBy {
     Parser.parseQuery("bool(1) < 2")
   } should have message "Invalid function name [bool]"
 }
}
