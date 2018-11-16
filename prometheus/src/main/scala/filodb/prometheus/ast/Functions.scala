package filodb.prometheus.ast

import filodb.query._

trait Functions extends Base with Operators with Vectors {

  case class Function(name: String, allParams: Seq[Expression]) extends Expression with PeriodicSeries {
    private val ignoreChecks = name.equalsIgnoreCase("vector") || name.equalsIgnoreCase("time")

    if (!ignoreChecks &&
      InstantFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      RangeFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty &&
      FiloFunctionId.withNameLowercaseOnlyOption(name.toLowerCase).isEmpty) {
      throw new IllegalArgumentException(s"Invalid function name [$name]")
    }

    def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      val seriesParam = allParams.filter(_.isInstanceOf[Series]).head.asInstanceOf[Series]
      val otherParams = allParams.filter(!_.isInstanceOf[Series]).map(_.asInstanceOf[Scalar].toScalar)
      val instantFunctionIdOpt = InstantFunctionId.withNameInsensitiveOption(name)
      val filoFunctionIdOpt = FiloFunctionId.withNameInsensitiveOption(name)

      if (instantFunctionIdOpt.isDefined) {
        val instantFunctionId = instantFunctionIdOpt.get
        val periodicSeriesPlan = seriesParam.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)

        ApplyInstantFunction(periodicSeriesPlan, instantFunctionId, otherParams)
      // Special FiloDB functions to extract things like chunk metadata
      } else if (filoFunctionIdOpt.isDefined) {
        // No lookback needed as we are looking at chunk metadata only, not raw samples
        val rangeSelector = timeParamToSelector(timeParams, 0)
        val (filters, columns) = seriesParam match {
          case i: InstantExpression => (i.columnFilters :+ i.nameFilter, i.columns)
          case r: RangeExpression   => (r.columnFilters :+ r.nameFilter, r.columns)
        }
        filoFunctionIdOpt.get match {
          case FiloFunctionId.ChunkMetaAll =>   // Just get the raw chunk metadata
            RawChunkMeta(rangeSelector, filters, columns.headOption.getOrElse(""))
        }
      } else {
        val rangeFunctionId = RangeFunctionId.withNameInsensitiveOption(name).get
        val rangeExpression = seriesParam.asInstanceOf[RangeExpression]

        PeriodicSeriesWithWindowing(
          rangeExpression.toRawSeriesPlan(timeParams, isRoot = false).asInstanceOf[RawSeries],
          timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000,
          rangeExpression.window.millis,
          rangeFunctionId, otherParams)
      }
    }

  }

}
