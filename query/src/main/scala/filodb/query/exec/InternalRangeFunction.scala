package filodb.query.exec

import filodb.query.RangeFunctionId

// Used for internal representations of RangeFunctions
sealed trait InternalRangeFunction

object InternalRangeFunction {
  case object AvgOverTime extends InternalRangeFunction

  case object Changes extends InternalRangeFunction

  case object CountOverTime extends InternalRangeFunction

  case object Delta extends InternalRangeFunction

  case object Deriv extends InternalRangeFunction

  case object HoltWinters extends InternalRangeFunction

  case object Idelta extends InternalRangeFunction

  case object Increase extends InternalRangeFunction

  case object Irate extends InternalRangeFunction

  case object MaxOverTime extends InternalRangeFunction

  case object MinOverTime extends InternalRangeFunction

  case object PredictLinear extends InternalRangeFunction

  case object QuantileOverTime extends InternalRangeFunction

  case object Rate extends InternalRangeFunction

  case object Resets extends InternalRangeFunction

  case object StdDevOverTime extends InternalRangeFunction

  case object StdVarOverTime extends InternalRangeFunction

  case object SumOverTime extends InternalRangeFunction

  // Used only for ds-gauge schema
  case object AvgWithSumAndCountOverTime extends InternalRangeFunction

  def lpToInternalFunc(extFuncId: RangeFunctionId): InternalRangeFunction = extFuncId match {
    case RangeFunctionId.AvgOverTime   => AvgOverTime
    case RangeFunctionId.Changes       => Changes
    case RangeFunctionId.CountOverTime => CountOverTime
    case RangeFunctionId.Delta         => Delta
    case RangeFunctionId.Deriv         => Deriv
    case RangeFunctionId.HoltWinters   => HoltWinters
    case RangeFunctionId.Idelta        => Idelta
    case RangeFunctionId.Increase      => Increase
    case RangeFunctionId.Irate         => Irate
    case RangeFunctionId.MaxOverTime   => MaxOverTime
    case RangeFunctionId.MinOverTime   => MinOverTime
    case RangeFunctionId.PredictLinear => PredictLinear
    case RangeFunctionId.QuantileOverTime => QuantileOverTime
    case RangeFunctionId.Rate          => Rate
    case RangeFunctionId.Resets        => Resets
    case RangeFunctionId.StdDevOverTime => StdDevOverTime
    case RangeFunctionId.StdVarOverTime => StdVarOverTime
    case RangeFunctionId.SumOverTime   => SumOverTime
  }
}

