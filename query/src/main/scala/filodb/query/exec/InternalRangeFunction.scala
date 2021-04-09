package filodb.query.exec

import enumeratum.EnumEntry

import filodb.query.RangeFunctionId

// Used for internal representations of RangeFunctions
sealed abstract class InternalRangeFunction(val onCumulCounter: Boolean = false) extends EnumEntry

object InternalRangeFunction {
  case object AvgOverTime extends InternalRangeFunction

  case object Changes extends InternalRangeFunction

  case object CountOverTime extends InternalRangeFunction

  case object Delta extends InternalRangeFunction

  case object Deriv extends InternalRangeFunction

  case object HoltWinters extends InternalRangeFunction

  case object ZScore extends InternalRangeFunction

  case object Idelta extends InternalRangeFunction

  case object Increase extends InternalRangeFunction(true)

  case object Irate extends InternalRangeFunction

  case object MaxOverTime extends InternalRangeFunction

  case object MinOverTime extends InternalRangeFunction

  case object PredictLinear extends InternalRangeFunction

  case object QuantileOverTime extends InternalRangeFunction

  case object Rate extends InternalRangeFunction(true)

  case object Resets extends InternalRangeFunction

  case object StdDevOverTime extends InternalRangeFunction

  case object StdVarOverTime extends InternalRangeFunction

  case object SumOverTime extends InternalRangeFunction

  case object Last extends InternalRangeFunction

  // Used only for ds-gauge schema
  case object AvgWithSumAndCountOverTime extends InternalRangeFunction

  // Used only for histogram schemas with max column
  case object SumAndMaxOverTime extends InternalRangeFunction
  case object LastSampleHistMax extends InternalRangeFunction

  case object Timestamp extends InternalRangeFunction

  case object AbsentOverTime extends InternalRangeFunction

  //scalastyle:off cyclomatic.complexity
  def lpToInternalFunc(extFuncId: RangeFunctionId): InternalRangeFunction = extFuncId match {
    case RangeFunctionId.AvgOverTime      => AvgOverTime
    case RangeFunctionId.Changes          => Changes
    case RangeFunctionId.CountOverTime    => CountOverTime
    case RangeFunctionId.Delta            => Delta
    case RangeFunctionId.Deriv            => Deriv
    case RangeFunctionId.HoltWinters      => HoltWinters
    case RangeFunctionId.ZScore           => ZScore
    case RangeFunctionId.Idelta           => Idelta
    case RangeFunctionId.Increase         => Increase
    case RangeFunctionId.Irate            => Irate
    case RangeFunctionId.MaxOverTime      => MaxOverTime
    case RangeFunctionId.MinOverTime      => MinOverTime
    case RangeFunctionId.PredictLinear    => PredictLinear
    case RangeFunctionId.QuantileOverTime => QuantileOverTime
    case RangeFunctionId.Rate             => Rate
    case RangeFunctionId.Resets           => Resets
    case RangeFunctionId.StdDevOverTime   => StdDevOverTime
    case RangeFunctionId.StdVarOverTime   => StdVarOverTime
    case RangeFunctionId.SumOverTime      => SumOverTime
    case RangeFunctionId.Timestamp        => Timestamp
    case RangeFunctionId.Last             => Last
    case RangeFunctionId.AbsentOverTime   => AbsentOverTime
  }
  //scalastyle:on cyclomatic.complexity
}

