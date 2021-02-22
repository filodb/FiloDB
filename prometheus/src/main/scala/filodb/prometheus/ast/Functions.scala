package filodb.prometheus.ast

import filodb.core.query.{ColumnFilter, RangeParams}
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
                  (!allParams(index).isInstanceOf[SubqueryExpression])
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
          val rangeSelector = timeParamToSelector(timeParams)
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
      } else {
        val funcId = RangeFunctionId.withNameInsensitiveOption(name).get
        if (funcId == Timestamp) {
          val instantExpression = seriesParam.asInstanceOf[InstantExpression]

          PeriodicSeriesWithWindowing(instantExpression.toRawSeriesPlan(timeParams),
            timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000, 0,
            funcId, false, otherParams, instantExpression.offset.map(_.millis(timeParams.step * 1000)))
        } else {
          seriesParam match {
            case re: RangeExpression => rangeExpressionArgumentLogicalPlan(re, timeParams, funcId, otherParams);
            case sq: SubqueryExpression => subqueryArgumentLogicalPlan(sq, timeParams, funcId, otherParams);
            case _ => ???
          }

        }
      }
    }

    def rangeExpressionArgumentLogicalPlan(
      rangeExpression : RangeExpression,
      timeParams: TimeRangeParams,
      rangeFunctionId: RangeFunctionId,
      otherParams: Seq[FunctionArgsPlan]
    ) : PeriodicSeriesPlan = {
      PeriodicSeriesWithWindowing(
        rangeExpression.toSeriesPlan(timeParams, isRoot = false),
        timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000,
        rangeExpression.window.duration.millis(timeParams.step * 1000),
        rangeFunctionId,
        rangeExpression.window.duration.timeUnit == IntervalMultiple,
        otherParams,
        rangeExpression.offset.map(_.millis(timeParams.step * 1000))
      )
    }

    def subqueryArgumentLogicalPlan(
      sq : SubqueryExpression,
      timeParams: TimeRangeParams,
      rangeFunctionId: RangeFunctionId,
      otherParams: Seq[FunctionArgsPlan]
    ) : PeriodicSeriesPlan = {
      val subqueriesPlan = sq.toSeriesPlan(timeParams)
      RangeFunctionPlan(rangeFunctionId, subqueriesPlan, timeParams.start, timeParams.step, timeParams.end)
    }

  }

  case class SubqueryExpression(subquery: PeriodicSeries, sqcl: SubqueryClause) extends Expression with PeriodicSeries {

    override def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      // If subquery is defined and the end is different from the start in TimeRangeParams, this means
      // that a subquery expression has been called by the query_range API.
      // Top level expression of a range query, however, should return an
      // instant vector but subqueries by definition return range vectors, hence, we check that and throw
      // exception. Star should be the same as end, and step should be 0.
      // Suppose we have
      //    sum_over_time(foo{}[60m])[1d:10m]
      // the above means we are to produce a range vector with 24*6 points which would be sum_over_time of 1h worth of
      // samples for each point.
      // TODO fix it for nested subqueries
      if (timeParams.start != timeParams.end) {
        throw new UnsupportedOperationException("Subquery is not allowed as a top level expression for query_range")
      }

      // Mosaic does not have a default step like prometheus does, will default o 60 seconds
      var stepToUse = 60L
      if (sqcl.step.isDefined) {
        stepToUse = sqcl.step.get.millis(1L) / 1000
      }

      // here we borrow TimeStepParams concept of a range query for subquery,
      // subquery in essence is a generalized range_query that can be executed
      // on each level of a query while range query can operate only on the top
      // level prometheus expression. With full implementation of subquery, query_range
      // API is obsolete because its capabilities are a subset of subquery capabilities
      var timeParamsToUse = TimeStepParams(
        timeParams.start - (sqcl.interval.millis(1L) / 1000),
        stepToUse,
        timeParams.start
      )

      // currently we want to support only subqueries over instant selector like:
      // foo[5m:1m]
      // and over time range functions like:
      // rate(foo[3m])[5m:1m]
      // eventually, we will start supporting subqueries in more cases
      subquery match {
        case ie: InstantExpression => ie.toSeriesPlan(timeParamsToUse);
        case f: Function => functionLogicalPlan(f, sqcl, timeParamsToUse);
        case _ => throw new IllegalArgumentException("Subqueries are supported only over functions and selectors")
      }
    }


    // In Phase II of subquery support in Mosaic we want to:
    //   (a) fully support range function subquery which argument is:
    //       an instant selector subquery, for example:
    //         rate(foo[5m:1m])[10m:1m]
    //       or range selector, for example:
    //         rate(foo[5m])[10m:1m]
    //       both of the cases are OPTIMIZED as they would produce
    //       a PeriodicSeriesWithWindow logical plan that will be executing
    //       only one actual physical C*/memory buffer query
    //   (b) support generation of proper logical plans for nested
    //       subqueries on another range function, for example:
    //         deriv( rate(distance_covered_meters_total[1m])[5m:1m] )[10m:]
    //       The above case is rather unusual and the motivation to support
    //       is to verify subquery logical plan design. Though we do generate
    //       logical plan, the actual implementation of the execution plans
    //       of nested subqueries might be finished in Phase III only
    //       Currently, the idea it to simply "materialize" each of the subqueries
    //       by generating appropriate logical plan for each of them.
    //       The problem with this approach is that the number of child logical plans can
    //       be in 100s or even 1000s. However, at this point we do not have a way to
    //       represent them without materializing them.
    //       One way of doing so would be a very complicated "subquery" logical plan
    //       that might look like:
    //         subquery( raw series, function1st level, function2nd level, function3rd level)
    def functionLogicalPlan(
      f: Function, sqcl: SubqueryClause, timeParams: TimeRangeParams
    ): PeriodicSeriesPlan = {

      f.allParams.head match {
        // for case (a) we need to verify that the argument of the function
        // is a range selector
        case re: RangeExpression => f.toSeriesPlan(timeParams)
        case sq: SubqueryExpression =>
          sq.subquery match {
            // case (a) for instant select subquery
            case InstantExpression(_, _, _) => f.toSeriesPlan(timeParams)
            // for case (b) we need to verify that the argument of the function
            // is a subquery over a range function
            case Function(_, _) => {
              val rf = RangeFunctionId.withNameInsensitiveOption(f.name)
              if (rf.isEmpty ) {
                throw new UnsupportedOperationException(
                  "Support only range functions as a parameter to a function subquery"
                )
              }
              // this is case (b)
              // Now we generate explicit Subquery that may or may not be further
              // optimized by the query planner
              val timeRangeParams = Seq[LogicalPlan]()
              import scala.collection.mutable.ArrayBuffer
              val starts = ArrayBuffer[Long]()
              var start = timeParams.start
              while (start <= timeParams.end) {
                starts.append(start)
                start += timeParams.step
              }
              val childPlans = starts.map( s => f.toSeriesPlan(TimeStepParams(s, 0, s)))
              Subquery(childPlans, timeParams.start, timeParams.step, timeParams.end)

            }
            case _ => throw new UnsupportedOperationException(
              "Support only range function and instant selector subqueries " +
                "as well as range selector for function subqueries"
            )
          }
      }
    }
  }

}
