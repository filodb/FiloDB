package filodb.query.exec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.reactive.Observable

import filodb.core.Utils
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => Utf8Str}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.BinaryOperator.{LAND, LOR, LUnless}

/**
  * Set operator between results of lhs and rhs plan.
  *
  * This ExecPlan accepts two sets of RangeVectors lhs and rhs from child plans.
  * It then does a join of the RangeVectors based on the fields of their keys as
  * dictated by `on` or `ignoring` fields passed as params.
  *
  * Joins can be Many-to-Many.
  *
  * @param lhs ExecPlan that will return results of LHS expression
  * @param rhs ExecPlan that will return results of RHS expression
  * @param binaryOp the binary operator
  * @param on fields from range vector keys to include while performing the join
  * @param ignoring fields from range vector keys to exclude while performing the join
  */
final case class SetOperatorExec(queryContext: QueryContext,
                                 dispatcher: PlanDispatcher,
                                 lhs: Seq[ExecPlan],
                                 rhs: Seq[ExecPlan],
                                 binaryOp: BinaryOperator,
                                 on: Seq[String],
                                 ignoring: Seq[String],
                                 metricColumn: String,
                                 outputRvRange: Option[RvRange]) extends NonLeafExecPlan {
  require(on == Nil || ignoring == Nil, "Cannot specify both 'on' and 'ignoring' clause")
  require(!on.contains(metricColumn), "On cannot contain metric name")

  private val onLabels = on.map(Utf8Str(_)).toSet
  private val ignoringLabels = ignoring.map(Utf8Str(_)).toSet + metricColumn.utf8
  // if onLabels is non-empty, we are doing matching based on on-label, otherwise we are
  // doing matching based on ignoringLabels even if it is empty

  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector] = ???

  protected[exec] def compose(childResponses: Observable[(QueryResult, Int)],
                              firstSchema: Task[ResultSchema],
                              querySession: QuerySession): Observable[RangeVector] = {
    val span = Kamon.currentSpan()
    val taskOfResults = childResponses.map {
      case (QueryResult(_, schema, result, _, _, _), i) => (schema, result, i)
    }.toListL.map { resp =>
      val startNs = Utils.currentCpuUserTimeNanos
      try {
        span.mark("binary-join-child-results-available")
        Kamon.histogram("query-execute-time-elapsed-step1-child-results-available",
          MeasurementUnit.time.milliseconds)
          .withTag("plan", getClass.getSimpleName)
          .record(Math.max(0, System.currentTimeMillis - queryContext.submitTime))
        // NOTE: We can't require this any more, as multischema queries may result in not a QueryResult if the
        //       filter returns empty results.  The reason is that the schema will be undefined.
        // require(resp.size == lhs.size + rhs.size, "Did not get sufficient responses for LHS and RHS")
        // Resp is segregated based on index of child plans
        val lhsRvs = resp.filter(_._3 < lhs.size).flatMap(_._2)
        val rhsResp = resp.filter(_._3 >= lhs.size)
        val rhsRvs = rhsResp.flatMap(_._2)

        val results: Iterator[RangeVector] = binaryOp match {
          case LAND => val rhsSchema = if (rhsResp.nonEmpty) rhsResp.head._1 else ResultSchema.empty
            setOpAnd(lhsRvs, rhsRvs, rhsSchema)
          case LOR => setOpOr(lhsRvs, rhsRvs)
          case LUnless => setOpUnless(lhsRvs, rhsRvs)
          case _ => throw new IllegalArgumentException("requirement failed: Only and, or and unless are supported ")
        }
        // check for timeout after dealing with metadata, before dealing with numbers
        querySession.qContext.checkQueryTimeout(this.getClass.getName)
        Observable.fromIteratorUnsafe(results)
      } finally {
        // Adding CPU time here since dealing with metadata join is not insignificant
        querySession.queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentCpuUserTimeNanos - startNs)
      }
    }
    Observable.fromTask(taskOfResults).flatten
  }

  private def joinKeys(rvk: RangeVectorKey): Map[Utf8Str, Utf8Str] = {
    if (onLabels.nonEmpty) rvk.labelValues.filter(lv => onLabels.contains(lv._1))
    else rvk.labelValues.filterNot(lv => ignoringLabels.contains(lv._1))
  }

  /***
    * Returns true when range vector does not have any values
    */
  private[exec] def isEmpty(rv: RangeVector, schema: ResultSchema) = {
    if (schema.isHistogram) rv.rows().forall(_.getHistogram(1).numBuckets == 0)
    else rv.rows().forall(_.getDouble(1).isNaN)
  }

  /***
   * Add LHS range vector in result which have matching join keys in RHS.
   * LHS row should only be added if corresponding row exists in RHS
   */
  // scalastyle:off method.length
  private[exec] def setOpAnd(lhsRvs: List[RangeVector], rhsRvs: List[RangeVector],
                       rhsSchema: ResultSchema): Iterator[RangeVector] = {
    // isEmpty method consumes rhs range vector
    require(rhsRvs.forall(_.isInstanceOf[SerializedRangeVector]), "RHS should be SerializedRangeVector")

    val result = new mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]]()
    val rhsMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()

    val period = lhsRvs.headOption.flatMap(_.outputRange)

    rhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      // Don't add range vector if it is empty
      if (jk.nonEmpty && !isEmpty(rv, rhsSchema)) {
        // When spread changes, we need to account for multiple Range Vectors with same key coming from different shards
        // Each of these range vectors would contain data for different time ranges
        if (rhsMap.contains(jk)) {
          val resVal = rhsMap(jk)
          if (resVal.key.labelValues == rv.key.labelValues) {
            rhsMap.put(jk, StitchRvsExec.stitch(rv, resVal, outputRvRange) )
          } else {
            rhsMap.put(jk, rv)
          }
        } else rhsMap.put(jk, rv)
      }
    }

    lhsRvs.foreach { lhs =>
      val jk = joinKeys(lhs.key)
      // Add range vectors from lhs which are present in lhs and rhs both or when jk is empty
      if (rhsMap.contains(jk)) {
        var index = -1
        val lhsStitched = if (result.contains(jk)) {
          val resVal = result(jk)
          // If LHS keys exist in result, stitch if it is a duplicate
          // While this may look like a nested loop scanning a lot of elements, in practice, this ArrayBuffer will only
          // have size > 1 when there is a change of spread and the same timeseries comes from two different shards
          index = resVal.indexWhere(r => r.key.labelValues == lhs.key.labelValues)
          if (index >= 0) {
             StitchRvsExec.stitch(lhs, resVal(index), outputRvRange)
          } else {
            lhs
          }
        } else lhs
        val lhsRows = lhsStitched.rows()
        val rhsRows = rhsMap(jk).rows()

        val rows: RangeVectorCursor = new RangeVectorCursor {
          val cur = new TransientRow()
          override def hasNext: Boolean = lhsRows.hasNext && rhsRows.hasNext
          override def next(): RowReader = {
            val lhsRow = lhsRows.next()
            val rhsRow = rhsRows.next()
            // LHS row should not be added to result if corresponding RHS row does not exist
            val res = if (rhsRow.getDouble(1).isNaN) Double.NaN else lhsRow.getDouble(1)
            cur.setValues(lhsRow.getLong(0), res)
            cur
          }
          override def close(): Unit = {
            lhsRows.close()
            rhsRows.close()
          }
        }
        val arrayBuffer = result.getOrElse(jk, ArrayBuffer())
        val resRv = IteratorBackedRangeVector(lhs.key, rows, period)
        if (index >= 0) arrayBuffer.update(index, resRv) else arrayBuffer.append(resRv)
        result.put(jk, arrayBuffer)
      } else if (jk.isEmpty) {
        // "up AND ON (dummy) vector(1)" should be equivalent to up as there's no dummy label
        val arrayBuffer = result.getOrElse(jk, ArrayBuffer())
        arrayBuffer.append(lhs)
        result.put(jk, arrayBuffer)
      }
    }
    result.valuesIterator.map(_.toIterator).flatten
  }
  // scalastyle:on method.length


  /***
   * Add all LHS range vector in result. From RHS only only add the range vectors which have join key not
   * present in result
   */
  private def setOpOr(lhsRvs: List[RangeVector]
                      , rhsRvs: List[RangeVector]): Iterator[RangeVector] = {

    val lhsResult = new mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]]()
    val rhsResult = new mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]]()

    // Add everything from left hand side range vector
    lhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
       if (lhsResult.contains(jk)) {
        val resVal = lhsResult(jk)
         // While this may look like a nested loop scanning a lot of elements, in practice, this ArrayBuffer will only
         // have size > 1 when there is a change of spread and the same timeseries comes from two different shards
        val index = resVal.indexWhere(r => r.key.labelValues == rv.key.labelValues)
        if (index >= 0) {
          val stitched = StitchRvsExec.stitch(rv, resVal(index), outputRvRange)
          resVal.update(index, stitched)
        } else {
          lhsResult(jk).append(rv)
        }
      } else {
         val lhsList = ArrayBuffer[RangeVector]()
         lhsList.append(rv)
         lhsResult.put(jk, lhsList)
      }
    }
    // Add range vectors from right hand side which are not present on lhs
    rhsRvs.foreach { rhs =>
      val jk = joinKeys(rhs.key)
      if (!lhsResult.contains(jk)) {
        if (rhsResult.contains(jk)) {
          val resVal = rhsResult(jk)
          // While this may look like a nested loop scanning a lot of elements, in practice, this ArrayBuffer will only
          // have size > 1 when there is a change of spread and the same timeseries comes from two different shards
          val index = resVal.indexWhere(r => r.key.labelValues == rhs.key.labelValues)
          if (index >= 0) {
            val stitched = StitchRvsExec.stitch(rhs, resVal(index), outputRvRange)
            resVal.update(index, stitched)
          } else {
            resVal.append(rhs)
          }
        } else {
          val rhsList = rhsResult.getOrElse(jk, ArrayBuffer())
          rhsList.append(rhs)
          rhsResult.put(jk, rhsList)
        }
      }
    }
    lhsResult.valuesIterator.map(_.toIterator).flatten ++ rhsResult.valuesIterator.map(_.toIterator).flatten
  }

  /***
   * Return LHS range vector which have join keys not present in RHS
   */
  private[exec] def setOpUnless(lhsRvs: List[RangeVector],
                          rhsRvs: List[RangeVector]): Iterator[RangeVector] = {

    val rhsKeysSet = new mutable.HashSet[Map[Utf8Str, Utf8Str]]()
    rhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      rhsKeysSet += jk
    }
    // Add range vectors which are not present in rhs

    val result = lhsRvs.foldLeft(
      mutable.Map.empty[Map[Utf8Str, Utf8Str], mutable.Map[Map[Utf8Str, Utf8Str], RangeVector]]) {
      case (accumulated, lhs) =>
        val jk = joinKeys(lhs.key)
        if (!rhsKeysSet.contains(jk)) {
          val result = accumulated.getOrElseUpdate(jk, mutable.Map.empty)
          val stitchedOrNewValue = result.get(lhs.key.labelValues) match {
            case Some(matched)  => StitchRvsExec.stitch(lhs, matched, outputRvRange)
            case None           => lhs
          }
          result(lhs.key.labelValues) = stitchedOrNewValue
        }
        accumulated
    }
    result.valuesIterator.flatMap( m => m.valuesIterator)
  }

}
