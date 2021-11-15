package filodb.prometheus.ast

import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.query._
import filodb.query.RangeFunctionId.Timestamp

case class Function(name: String, allParams: Seq[Expression]) extends Expression with PeriodicSeries {
  private val ignoreChecks = name.equalsIgnoreCase("vector") || name.equalsIgnoreCase("time")

  if (!ignoreChecks &&
    InstantFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    RangeFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    FiloFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    MiscellaneousFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    ScalarFunctionId.withNameInsensitiveOption(name.toLowerCase).isEmpty &&
    SortFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    AbsentFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
    LimitFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty) {

    throw new IllegalArgumentException(s"Invalid function name [$name]")
  }

  // Below code is for validating the syntax of promql functions belonging to RangeFunctionID.
  // It takes care of validating syntax of the tokenized input query before creating the logical plan.
  // In case of invalid params/invalid syntax, then throw exceptions with similar error-messages like promql.

  // error messages
  val errWrongArgumentCount = "argument(s) in call to function "

  val functionId = RangeFunctionId.withNameLowercaseOnlyOption(name.toLowerCase)
  if (functionId.nonEmpty) {
    val funcName = functionId.get.entryName
    // get the parameter spec of the function from RangeFunctionID
    val paramSpec = functionId.get.paramSpec

    // if the length of the args in param spec != to the args in
    // the i/p query, then the i/p query is INCORRECT,
    // throw invalid no. of args exception.
    if (paramSpec.length != allParams.length)
      throw new IllegalArgumentException(s"Expected ${paramSpec.length} " +
        s"$errWrongArgumentCount $funcName, got ${allParams.size}")

    // if length of param spec and all params is same,
    // then check the type of each argument and check the order of the arguments.
    else {
      paramSpec.zipWithIndex.foreach {
        case (specType, index) => specType match {
          case RangeVectorParam(errorMsg) =>
            if (
              !allParams(index).isInstanceOf[RangeExpression] &&
              !allParams(index).isInstanceOf[SubqueryExpression]
            ) throw new IllegalArgumentException(s"$errorMsg $funcName, " +
              s"got ${allParams(index).getClass.getSimpleName}")

          case InstantVectorParam(errorMsg) =>
            if (!allParams(index).isInstanceOf[InstantExpression])
              throw new IllegalArgumentException(s"$errorMsg $funcName, " +
                s"got ${allParams(index).getClass.getSimpleName}")

          case ScalarParam(errorMsg) =>
            if (!allParams(index).isInstanceOf[ScalarExpression])
              throw new IllegalArgumentException(s"$errorMsg $funcName, " +
                s"got ${allParams(index).getClass.getSimpleName}")

          case ScalarRangeParam(min, max, errorMsg) =>
            val paramObj = allParams(index)
            // Function like "Holt-winters" needs trend & smoothing factor between 0 and 1.
            // If the obj is Scalar Expression, validate the value of the obj to be between 0 and 1.
            // If the obj is not Scalar Expression, then throw exception.
            if (!paramObj.isInstanceOf[ScalarExpression])
              throw new IllegalArgumentException(s"$errorMsg $funcName, " +
                s"got ${allParams(index).getClass.getSimpleName}")

            else {
              val paramValue = paramObj.asInstanceOf[ScalarExpression].toScalar
              if (!(paramValue > min && paramValue < max))
                throw new IllegalArgumentException(s"$errorMsg $paramValue")
            }

          case _ => throw new IllegalArgumentException("Invalid Query")
        }
      }
    }
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
                 .filter(!_.isInstanceOf[StringLiteral])
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
        val rangeSelector = Base.timeParamToSelector(timeParams)
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

  // scalastyle:off method.length
  def toSeriesPlanMisc(seriesParam: Series,
                       otherParams: Seq[FunctionArgsPlan],
                       timeParams: TimeRangeParams): PeriodicSeriesPlan = {

    val miscellaneousFunctionIdOpt = MiscellaneousFunctionId.withNameInsensitiveOption(name)
    val scalarFunctionIdOpt = ScalarFunctionId.withNameInsensitiveOption(name)
    val sortFunctionIdOpt = SortFunctionId.withNameInsensitiveOption(name)
    val absentFunctionIdOpt = AbsentFunctionId.withNameInsensitiveOption(name)
    val limitFunctionIdOpt = LimitFunctionId.withNameInsensitiveOption(name)

    if (miscellaneousFunctionIdOpt.isDefined) {
      // Get parameters other than  series like label names. Parameters can be quoted so remove special characters
      val stringParam = allParams.filter(!_.equals(seriesParam)).collect {
        case e: InstantExpression => e.realMetricName.replaceAll("^\"|\"$", "")
        case s: StringLiteral => s.str
      }
      val miscellaneousFunctionId = miscellaneousFunctionIdOpt.get
      val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
      ApplyMiscellaneousFunction(periodicSeriesPlan, miscellaneousFunctionId, stringParam)
    } else if (scalarFunctionIdOpt.isDefined) {
      val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
      ScalarVaryingDoublePlan(periodicSeriesPlan, scalarFunctionIdOpt.get)
    } else if (sortFunctionIdOpt.isDefined) {
      val sortFunctionId = sortFunctionIdOpt.get
      val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
      ApplySortFunction(periodicSeriesPlan, sortFunctionId)
    } else if (absentFunctionIdOpt.isDefined) {
      val columnFilter = if (seriesParam.isInstanceOf[InstantExpression])
        seriesParam.asInstanceOf[InstantExpression].columnFilters else Seq.empty[ColumnFilter]
      val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
      ApplyAbsentFunction(periodicSeriesPlan, columnFilter, RangeParams(timeParams.start, timeParams.step,
        timeParams.end))
    } else if (limitFunctionIdOpt.isDefined) {
      val limit = allParams(0).asInstanceOf[Scalar].toScalar.toInt
      val columnFilter = if (seriesParam.isInstanceOf[InstantExpression])
        seriesParam.asInstanceOf[InstantExpression].columnFilters else Seq.empty[ColumnFilter]
      val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toSeriesPlan(timeParams)
      ApplyLimitFunction(periodicSeriesPlan, columnFilter, RangeParams(timeParams.start, timeParams.step,
        timeParams.end), limit)
    } else {
      val rangeFunctionId = RangeFunctionId.withNameInsensitiveOption(name).get
      if (rangeFunctionId == Timestamp) {
        val instantExpression = seriesParam.asInstanceOf[InstantExpression]

        PeriodicSeriesWithWindowing(instantExpression.toRawSeriesPlan(timeParams),
          timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000, 0,
          rangeFunctionId, false, otherParams, instantExpression.offset.map(_.millis(timeParams.step * 1000)))
      } else {
        seriesParam match {
          case re: RangeExpression => rangeExpressionArgument(re, timeParams, rangeFunctionId, otherParams)
          case sq: SubqueryExpression => subqueryArgument(sq, timeParams, rangeFunctionId, otherParams)
        }
      }
    }
  }

  def rangeExpressionArgument(
    rangeExpression : RangeExpression,
    timeParams: TimeRangeParams,
    rangeFunctionId: RangeFunctionId,
    otherParams: Seq[FunctionArgsPlan]
  ) : PeriodicSeriesPlan = {
    PeriodicSeriesWithWindowing(
      rangeExpression.toSeriesPlan(timeParams, isRoot = false),
      timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000,
      rangeExpression.window.millis(timeParams.step * 1000),
      rangeFunctionId,
      rangeExpression.window.timeUnit == IntervalMultiple,
      otherParams,
      rangeExpression.offset.map(_.millis(timeParams.step * 1000)),
      rangeExpression.columnFilters
    )
  }

  def subqueryArgument(
    sqe : SubqueryExpression,
    timeParams: TimeRangeParams,
    rangeFunctionId: RangeFunctionId,
    otherParams: Seq[FunctionArgsPlan]
  ) : PeriodicSeriesPlan = {
    var subqueryStepToUseMs = SubqueryUtils.getSubqueryStepMs(sqe.sqcl.step)
    // when start and stop are the same, step should be zero too
    var outerStepMs = timeParams.step * 1000
    if (timeParams.start == timeParams.end) {
      outerStepMs = 0
    }
    val stepForInnerMs = subqueryStepToUseMs
    val offsetMs = sqe.offset.map { _.millis(1L) }
    val offsetSecForInner = offsetMs match {
      case None => 0
      case Some(ofMs) => ofMs / 1000
    }

//    val timeParamsWithAt = if (sqe.at.nonEmpty) {
//      val endSec = sqe.at.get.getUnix(timeParams)
//      val startSec = endSec - (timeParams.end - timeParams.start)
//      TimeStepParams(startSec, timeParams.step, endSec)
//    } else {
//      timeParams
//    }

    val preciseStartForInnerS = timeParams.start - (sqe.sqcl.window.millis(1L) / 1000) - offsetSecForInner
    val startForInnerS = SubqueryUtils.getStartForFastSubquery(preciseStartForInnerS, subqueryStepToUseMs/1000)

    val preciseEndForInnerS = timeParams.end - offsetSecForInner
    val endForInnerS =
      SubqueryUtils.getEndForFastSubquery(preciseEndForInnerS, subqueryStepToUseMs/1000)

    val timeParamsForInner = TimeStepParams(
      startForInnerS,
      stepForInnerMs/1000,
      endForInnerS
    )
    // We don't want to naively execute subquery by concatenating
    // the results of multiple queries. Instead, the inner subqueries are executed
    // as a wider single query with newly calculated start and step. Later we
    // apply a transformation against the resulting time series to form the actual
    // response of the subquery.
    if (sqe.at.isEmpty) {
      val subquery = sqe.subquery.toSeriesPlan(timeParamsForInner)
      SubqueryWithWindowing(
        subquery,
        timeParams.start * 1000, outerStepMs, timeParams.end * 1000,
        rangeFunctionId,
        otherParams,
        sqe.sqcl.window.millis(1L),
        subqueryStepToUseMs,
        offsetMs
      )
    } else {
      val offsetSec = offsetMs.getOrElse(0L) / 1000

      // limit range start/end
      val sqStartSec = sqe.at.get.getUnix(timeParams) - offsetSec
      val sqEndSec = sqStartSec
      val sqStepSec = timeParams.step  // TODO(a_theimer): doesn't matter?

      // limit periodic start/end
      val innerEndSec = sqEndSec
      val innerStartSec = innerEndSec - (sqe.sqcl.window.millis(1L) / 1000)
      val innerStepSec = subqueryStepToUseMs / 1000

      val inner = sqe.subquery.toSeriesPlan(
        TimeStepParams(innerStartSec, innerStepSec, innerEndSec))

      val sq = SubqueryWithWindowing(
        inner,
        sqStartSec * 1000, sqStepSec * 1000, sqEndSec * 1000,
        rangeFunctionId,
        otherParams,
        sqe.sqcl.window.millis(1L),
        subqueryStepToUseMs,
        offsetMs
      )

      new AtSeriesPeriodic(sq, timeParams.start * 1000, timeParams.step * 1000,
                           timeParams.end * 1000, sqStartSec * 1000)
    }

  }
  // scalastyle:on method.length
}
