package filodb.prometheus.ast

import filodb.core.query.RangeParams
import filodb.query._
import filodb.query.RangeFunctionId.Timestamp

trait Functions extends Base with Operators with Vectors {

  case class Function(name: String, allParams: Seq[Expression]) extends Expression with PeriodicSeries {
    private val ignoreChecks = name.equalsIgnoreCase("vector") || name.equalsIgnoreCase("time")

    if (!ignoreChecks &&
      InstantFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      RangeFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      FiloFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      MiscellaneousFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      ScalarFunctionId.withNameInsensitiveOption(name.toLowerCase).isEmpty &&
      SortFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      AbsentFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty) {

      throw new IllegalArgumentException(s"Invalid function name [$name]")
    }

    /**
      *
      * @return true when function is scalar or time
      */
    def isScalarFunction(): Boolean =
      name.equalsIgnoreCase("scalar") ||
      name.equalsIgnoreCase("time")

    def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      val vectorFn = VectorFunctionId.withNameInsensitiveOption(name)
      val instantFunctionIdOpt = InstantFunctionId.withNameInsensitiveOption(name)
      val filoFunctionIdOpt = FiloFunctionId.withNameInsensitiveOption(name)
      val scalarFunctionIdOpt = ScalarFunctionId.withNameInsensitiveOption(name)
      if (vectorFn.isDefined) {
        allParams.head match {
          case num: ScalarExpression => val params = RangeParams(timeParams.start, timeParams.step, timeParams.end)
                                        VectorPlan(ScalarFixedDoublePlan(num.toScalar, params))
          case function: Function    => val nestedPlan = function.toSeriesPlan(timeParams)
                                        nestedPlan match {
                                          case scalarPlan: ScalarPlan => VectorPlan(scalarPlan)
                                          case _                      => throw new UnsupportedOperationException()
                                        }
        }
      } else if (allParams.isEmpty) {
        ScalarTimeBasedPlan(scalarFunctionIdOpt.get, RangeParams(timeParams.start, timeParams.step, timeParams.end) )
      } else {
        val seriesParam = allParams.filter(_.isInstanceOf[Series]).head.asInstanceOf[Series]
        val otherParams: Seq[FunctionArgsPlan] =
          allParams.filter(!_.equals(seriesParam))
                   .filter(!_.isInstanceOf[InstantExpression])
                   .map {
                     case num: ScalarExpression =>
                       val params = RangeParams(timeParams.start, timeParams.step, timeParams.end)
                       ScalarFixedDoublePlan(num.toScalar, params)
                     case function: Function if (function.name.equalsIgnoreCase("scalar")) =>
                       function.toSeriesPlan(timeParams).asInstanceOf[ScalarVaryingDoublePlan]
                     case _ =>
                       throw new IllegalArgumentException("Parameters can be a string, number or scalar function")
                   }
        if (instantFunctionIdOpt.isDefined) {
          val instantFunctionId = instantFunctionIdOpt.get
          val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
          ApplyInstantFunction(periodicSeriesPlan, instantFunctionId, otherParams)
          // Special FiloDB functions to extract things like chunk metadata
        } else if (filoFunctionIdOpt.isDefined) {
          // No lookback needed as we are looking at chunk metadata only, not raw samples
          val rangeSelector = timeParamToSelector(timeParams, 0)
          val (filters, column) = seriesParam match {
            case i: InstantExpression => (i.columnFilters, i.column)
            case r: RangeExpression   => (r.columnFilters, r.column)
          }
          filoFunctionIdOpt.get match {
            case FiloFunctionId.ChunkMetaAll =>   // Just get the raw chunk metadata
              RawChunkMeta(rangeSelector, filters, column.getOrElse(""))
          }
        } else toSeriesPlanMisc(seriesParam, otherParams, timeParams)
      }
    }

    def toSeriesPlanMisc(seriesParam: Series,
                                 otherParams: Seq[FunctionArgsPlan],
                                 timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      val miscellaneousFunctionIdOpt = MiscellaneousFunctionId.withNameInsensitiveOption(name)
      val scalarFunctionIdOpt = ScalarFunctionId.withNameInsensitiveOption(name)
      val sortFunctionIdOpt = SortFunctionId.withNameInsensitiveOption(name)
      val absentFunctionIdOpt = AbsentFunctionId.withNameInsensitiveOption(name)
      // Get parameters other than  series like label names. Parameters can be quoted so remove special characters
      val stringParam = allParams.filter(!_.equals(seriesParam)).collect {
        case e: InstantExpression => e.realMetricName.replaceAll("^\"|\"$", "")
      }

      if (miscellaneousFunctionIdOpt.isDefined) {
        val miscellaneousFunctionId = miscellaneousFunctionIdOpt.get
        val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
        ApplyMiscellaneousFunction(periodicSeriesPlan, miscellaneousFunctionId, stringParam)
      } else if (scalarFunctionIdOpt.isDefined) {
        val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
        val params = RangeParams(timeParams.start, timeParams.step, timeParams.end)
        ScalarVaryingDoublePlan(periodicSeriesPlan, scalarFunctionIdOpt.get, params)
      } else if (sortFunctionIdOpt.isDefined) {
        val sortFunctionId = sortFunctionIdOpt.get
        val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
        ApplySortFunction(periodicSeriesPlan, sortFunctionId)
      } else if (absentFunctionIdOpt.isDefined) {
        val columnFilter = seriesParam.asInstanceOf[InstantExpression].columnFilters
        val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
        ApplyAbsentFunction(periodicSeriesPlan, columnFilter, RangeParams(timeParams.start, timeParams.step,
          timeParams.end))
      } else {
        val rangeFunctionId = RangeFunctionId.withNameInsensitiveOption(name).get
        if (rangeFunctionId == Timestamp) {
          val instantExpression = seriesParam.asInstanceOf[InstantExpression]
          val offsetMillis: Long = instantExpression.offset.map(_.millis).getOrElse(0)

          PeriodicSeriesWithWindowing(instantExpression.toRawSeriesPlan(timeParams),
            timeParams.start * 1000 - offsetMillis, timeParams.step * 1000, timeParams.end * 1000 - offsetMillis, 0,
            rangeFunctionId, otherParams, instantExpression.offset.map(_.millis))
        } else {
          val rangeExpression = seriesParam.asInstanceOf[RangeExpression]
          val offsetMillis: Long = rangeExpression.offset.map(_.millis).getOrElse(0)

          PeriodicSeriesWithWindowing(
            rangeExpression.toSeriesPlan(timeParams, isRoot = false),
            timeParams.start * 1000 - offsetMillis, timeParams.step * 1000, timeParams.end * 1000 - offsetMillis,
            rangeExpression.window.millis,
            rangeFunctionId, otherParams, rangeExpression.offset.map(_.millis))
        }
      }
    }
  }
}