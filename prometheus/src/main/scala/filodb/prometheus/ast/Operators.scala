package filodb.prometheus.ast

import filodb.query.BinaryOperator

 /*
  * The following label matching operators exist:
  * = Select labels that are exactly equal to the provided string.
  * =: Select labels that are exactly equal to the provided string.
  * !=: Select labels that are not equal to the provided string.
  * =~: Select labels that regex-match the provided string (or substring).
  * !~: Select labels that do not regex-match the provided string (or substring).
  * *
  * The following binary comparison operators exist in Prometheus:
  * == (equal)
  * != (not-equal)
  * > (greater-than)
  * < (less-than)
  * >= (greater-or-equal)
  * <= (less-or-equal)
  *
  * Set Operators are Or, And and Unless
  */
trait Operators {

  sealed trait PromToken

  sealed trait Operator extends PromToken {
    def getPlanOperator: BinaryOperator
  }

  case object EqualMatch extends Operator {
    override def getPlanOperator: BinaryOperator = BinaryOperator.EQL
  }

  case object RegexMatch extends Operator {
    override def getPlanOperator: BinaryOperator = BinaryOperator.EQLRegex
  }

  case object NotRegexMatch extends Operator {
    override def getPlanOperator: BinaryOperator = BinaryOperator.NEQRegex
  }

  sealed trait Comparision extends Operator {
    def isBool: Boolean
  }

  case class NotEqual(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.NEQ else BinaryOperator.NEQ_BOOL
  }

  case class Eq(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.EQL else BinaryOperator.EQL_BOOL
  }

  case class Gt(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.GTR else BinaryOperator.GTR_BOOL
  }

  case class Gte(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.GTE else BinaryOperator.GTE_BOOL
  }

  case class Lt(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.LSS else BinaryOperator.LSS_BOOL
  }

  case class Lte(isBool: Boolean) extends Comparision {
    override def getPlanOperator: BinaryOperator = if (!isBool) BinaryOperator.LTE else BinaryOperator.LTE_BOOL
  }

  case class LabelMatch(label: String, labelMatchOp: Operator, value: String) extends PromToken

  sealed trait ArithmeticOp extends Operator

  case object Add extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.ADD
  }

  case object Sub extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.SUB
  }

  case object Mul extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.MUL
  }

  case object Div extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.DIV
  }

  case object Mod extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.MOD
  }

  case object Pow extends ArithmeticOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.POW
  }

  sealed trait SetOp extends Operator

  case object And extends SetOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.LAND
  }

  case object Or extends SetOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.LOR
  }

  case object Unless extends SetOp {
    override def getPlanOperator: BinaryOperator = BinaryOperator.LUnless
  }

}

