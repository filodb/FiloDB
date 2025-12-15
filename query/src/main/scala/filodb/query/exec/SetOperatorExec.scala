package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import kamon.Kamon
import monix.eval.Task
import monix.reactive.Observable

import filodb.core.Utils
import filodb.core.metrics.FilodbMetrics
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => Utf8Str}
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
                                 on: Option[Seq[String]],
                                 ignoring: Seq[String],
                                 metricColumn: String,
                                 outputRvRange: Option[RvRange]) extends NonLeafExecPlan with BinaryJoinLikeExec {

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
      case (QueryResult(_, schema, result, _, _, _, _), i) => (schema, result, i)
    }.toListL.map { resp =>
      val startNs = Utils.currentThreadCpuTimeNanos
      try {
        span.mark("binary-join-child-results-available")
        FilodbMetrics.timeHistogram("query-execute-time-elapsed-step1-child-results-available", TimeUnit.MILLISECONDS,
          Map("plan" -> getClass.getSimpleName))
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
            setOpAnd(lhsRvs, rhsRvs, rhsSchema, querySession)
          case LOR => setOpOr(lhsRvs, rhsRvs)
          case LUnless => val rhsSchema = if (rhsResp.nonEmpty) rhsResp.head._1 else ResultSchema.empty
            setOpUnless(lhsRvs, rhsRvs, rhsSchema, querySession)
          case _ => throw new IllegalArgumentException("requirement failed: Only and, or and unless are supported ")
        }
        // check for timeout after dealing with metadata, before dealing with numbers
        querySession.qContext.checkQueryTimeout(this.getClass.getName)
        Observable.fromIteratorUnsafe(results)
      } finally {
        // Adding CPU time here since dealing with metadata join is not insignificant
        querySession.queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
      }
    }
    Observable.fromTask(taskOfResults).flatten
  }



  /***
    * Returns true when range vector does not have any values
    */
  private[exec] def isEmpty(rv: RangeVector, schema: ResultSchema) = {
    if (schema.isHistogram) rv.rows().forall(_.getHistogram(1).numBuckets == 0)
    else rv.rows().forall(x => java.lang.Double.isNaN(x.getDouble(1)))
  }

  // TODO: Note that these SetOperations don't work well with histograms
  //  Check behavior in prometheus and accordingly implement

  /***
   * Add LHS range vector in result which have matching join keys in RHS.
   * LHS row should only be added if corresponding row exists in RHS
   */
  // scalastyle:off method.length
  private[exec] def setOpAnd(lhsRvs: List[RangeVector], rhsRvs: List[RangeVector],
                       rhsSchema: ResultSchema, querySession: QuerySession): Iterator[RangeVector] = {

    lazy val builder = SerializedRangeVector.newBuilder()
    lazy val recSchema = SerializedRangeVector.toSchema(rhsSchema.columns, rhsSchema.brSchemas)
    lazy val execPlanName = queryWithPlanName(queryContext)
    val mappedRhsRvs = rhsRvs.map {
      case srv: SerializedRangeVector  => srv
      case rv: RangeVector               => SerializedRangeVector.apply(rv, builder,
                                            recSchema, execPlanName, querySession.queryStats)
    }

    val result = new mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]]()
    val rhsMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()

    val period = lhsRvs.headOption.flatMap(_.outputRange)

    mappedRhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      // Don't add range vector if it contains no rows
      // TODO: isEmpty is expensive and iterates all the rows, we need a performant implementation, we may very well not
      //  keep this check and it will do less iterations in case where we dont have several empty Range vectors
      if (!isEmpty(rv, rhsSchema)) {
        // When spread changes, we need to account for multiple Range Vectors with same key coming from different shards
        // Each of these range vectors would contain data for different time ranges
        if (rhsMap.contains(jk)) {
          val resVal = rhsMap(jk)
          if (resVal.key.labelValues == rv.key.labelValues) {
            rhsMap.put(jk, StitchRvsExec.stitch(rv, resVal, outputRvRange) )
          } else {
            // TODO: Is it ok to overwrite the rhs of multiple RVs match the joinKey
            //  Consider the case where two Range vectors match the join key and one of them has a non NaN value while
            //  while other one has a NaN value for a given time step and the one with a non NaN value is the final RV
            //  in the map. When we run this logic
            //    val res = if (rhsRow.getDouble(1).isNaN) Double.NaN else lhsRow.getDouble(1) the rhs is not NaN and
            //  assuming LHS is non NaN too, we will have a result for that given timestep but one of trhe matching RHS
            //  timeseries for that time is NaN. Test this case with prometheus and revisit
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
            val res = if (java.lang.Double.isNaN(rhsRow.getDouble(1))) Double.NaN else lhsRow.getDouble(1)
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
      }
    }
    result.valuesIterator.map(_.toIterator).flatten
  }
  // scalastyle:on method.length


  //scalastyle:off method.length
  /***
   * Add all LHS range vector in result. From RHS only only add the range vectors which have join key not
   * present in result
   */
  private def setOpOr(lhsRvs: List[RangeVector]
                      , rhsRvs: List[RangeVector]): Iterator[RangeVector] = {

    val lhsResult = groupAndStitchRangeVectors(lhsRvs)
    val rhsResult = groupAndStitchRangeVectors(rhsRvs)
    // All lhsResults should be considered
    // All rhsResults whose joinKey is not present in lhsResults should be considered
    // While iterating all lhs rangevectors for a time instant where none of the lhs range vectors are having a value
    // all RHS range vectors for that time instant having a matching join key will have a value in o/p

    //lhsResult.valuesIterator.map(_.toIterator).flatten ++ rhsResult.valuesIterator.map(_.toIterator).flatten

    // Create a Map of join key as key and value is a two tuple where _1 is all LHS Rvs for the join key and _2
    // are all RHS Rvs for that same join key
    val lRvs = lhsResult.foldLeft(Map.empty[Map[Utf8Str, Utf8Str], (Seq[RangeVector], Seq[RangeVector])]) {
      case (acc, (jk, rvs)) => acc + (jk -> ((rvs, Seq.empty[RangeVector])))
    }

    val joinedRvs = rhsResult.foldLeft(lRvs) {
      case (acc, (jk, rvs))  =>
        val newVal = acc.get(jk).map {
            // RHS will be Nil
          case (lhs, rhs) => (lhs, rvs)
        }.getOrElse((Nil, rvs))
        acc + (jk -> newVal)
    }

    case class OrSetOpIteratorWrapper(lhs: Seq[RangeVector], rhs: Seq[RangeVector]) {
      // Has a side effect where LHS are consumed first and a bit is set to indicate the time instant where there
      // is at least one non Nan value. the rhs values are then iterated over and emitted only where the given timestep
      // has no value in any of the LHS timeseries
      assert(outputRvRange.isDefined, "Expected to see a non null outputRvRange")
      val (start, end, step) = (outputRvRange.get.startMs, outputRvRange.get.endMs, outputRvRange.get.stepMs)
      // 64 bits in one word (long), 1 bit per step
      assert(start == end || step > 0, "Expected to see a non 0 step size when start and end are not same")
      val wordsRequired = if (start == end) 1 else ((((end - start) / step) >> 6) + 1).toInt
      val bitSet = new mutable.BitSet(wordsRequired)

      def joined(): Seq[RangeVector] = {
        val lhsRvs: Seq[RangeVector] = lhs.map(x => {
          var idx = 0
          val rows = x.rows()
          val rowsCursor: RangeVectorCursor = new RangeVectorCursor {
            val cur = new TransientRow()

            override def hasNext: Boolean = rows.hasNext

            override def next(): RowReader = {
              val reader = rows.next()
              // TODO: What is the expected behavior for Histograms?
              val value = reader.getDouble(1)
              val res = if (java.lang.Double.isNaN(value)) {
                Double.NaN
              } else {
                bitSet += idx
                value
              }
              idx += 1
              cur.setValues(reader.getLong(0), res)
              cur
            }

            override def close(): Unit = {
              rows.close()
            }
          }
          IteratorBackedRangeVector(x.key, rowsCursor, x.outputRange)
        })

        val rhsRvs: Seq[RangeVector] = rhs.map(x => {
          var idx = 0
          val rows = x.rows()
          val rowsCursor: RangeVectorCursor = new RangeVectorCursor {
            val cur = new TransientRow()

            override def hasNext: Boolean = rows.hasNext

            override def next(): RowReader = {
              val reader = rows.next()
              // TODO: What is the expected behavior for Histograms?
              val res = if (bitSet.contains(idx)) {
                Double.NaN
              } else {
                reader.getDouble(1)
              }
              idx += 1
              cur.setValues(reader.getLong(0), res)
              cur
            }

            override def close(): Unit = {
              rows.close()
            }
          }
          IteratorBackedRangeVector(x.key, rowsCursor, x.outputRange)
        })

        val lhsRvMap = lhsRvs.foldLeft(Map.empty[Map[Utf8Str, Utf8Str], List[RangeVector]]) {
          case (acc, rv) =>
            val key = rv.key.labelValues
            val groupedRvs = acc.get(key).map(rv :: _).getOrElse(List(rv))
            acc + (key -> groupedRvs)
        }

        // Dedupe the LHS and RHS rvs by keys, if same key is found for multiple RVs, stitch them
        // For e.g sum(foo{}) OR sum(bar{}), both have empty RV Key, we want them to return one RV
        // instead of two both with empty RV Keys
        val mergedGroupedRvs = rhsRvs.foldLeft(lhsRvMap){
          case (acc, rv)  =>
          val key = rv.key.labelValues
          acc.get(key) match {
            case Some(lhsRvs)   => acc + (key -> (rv :: lhsRvs))
            case None           => acc
          }
        }

        mergedGroupedRvs.map {
          case (_, rv :: Nil)   => rv
          case (key, rvs)       => IteratorBackedRangeVector(CustomRangeVectorKey(key),
                                        StitchRvsExec.merge(rvs.reverse.map(_.rows()), outputRvRange),
                                        outputRvRange)
        }.toSeq ++ rhsRvs.filter(rv => !mergedGroupedRvs.contains(rv.key.labelValues))
      }
    }


    joinedRvs.values.flatMap {
      // Case where only lhs rvs are present but no matching Rvs on rhs, just return the lhs
      case (lhs, Nil)  => lhs
      // Case where only rhs rvs are present but no matching Rvs on lhs, just return the rhs
      case (Nil, rhs)  => rhs
      // More involved case where the there will be a value for a time instant for rhs ONLY IF ALL rvs on lhs have no
      // value defined for that time instant
      case (lhs, rhs)  =>
        OrSetOpIteratorWrapper(lhs, rhs).joined()
    }.toIterator
  }
  //scalastyle:on method.length

  /**
   * Takes a List of rangevectors and groups them in Map grouped by the join key, if the Range vector with same labels
   * is encountered multiple times (possibly diuring spread change), the two range vectors will be deduplicated by
   * stitching
   *
   * @param rangeVectors List of range vectors
   * @return Map grouped by the joinKey and the value is a list of range vectors sharing the same join key
   */
  private def groupAndStitchRangeVectors(rangeVectors: List[RangeVector]):
    mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]] = {
    val results = new mutable.HashMap[Map[Utf8Str, Utf8Str], ArrayBuffer[RangeVector]]()
    rangeVectors.foreach { rv =>
      val jk = joinKeys(rv.key)
      if (results.contains(jk)) {
        val resVal = results(jk)
        // While this may look like a nested loop scanning a lot of elements, in practice, this ArrayBuffer will only
        // have size > 1 when there is a change of spread and the same timeseries comes from two different shards
        val index = resVal.indexWhere(r => r.key.labelValues == rv.key.labelValues)
        if (index >= 0) {
          val stitched = StitchRvsExec.stitch(rv, resVal(index), outputRvRange)
          resVal.update(index, stitched)
        } else {
          results(jk).append(rv)
        }
      } else {
        val list = ArrayBuffer[RangeVector]()
        list.append(rv)
        results.put(jk, list)
      }
    }
    results
  }

  /***
   * Return LHS range vector which have join keys not present in RHS
   */
  // scalastyle:off method.length
  private[exec] def setOpUnless(
                                 lhsRvs: List[RangeVector],
                                 rhsRvs: List[RangeVector],
                                 rhsSchema: ResultSchema,
                                 querySession: QuerySession
                               ): Iterator[RangeVector] = {

    // 1) Prep the serializer
    lazy val builder      = SerializedRangeVector.newBuilder()
    lazy val recSchema    = SerializedRangeVector.toSchema(rhsSchema.columns, rhsSchema.brSchemas)
    lazy val execPlanName = queryWithPlanName(queryContext)

    // 2) Turn every RHS into a SerializedRangeVector
    val mappedRhs = rhsRvs.map {
      case srv: SerializedRangeVector => srv
      case rv: RangeVector =>
        SerializedRangeVector(rv, builder, recSchema, execPlanName, querySession.queryStats)
    }

    // 3) Stitch shards of RHS into a single RV per join-key
    val rhsMap  = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()
    mappedRhs.foreach { rv =>
      val jk = joinKeys(rv.key)
      if (!isEmpty(rv, rhsSchema)) {
        rhsMap.get(jk) match {
          case Some(resVal) if resVal.key.labelValues == rv.key.labelValues =>
            rhsMap.put(jk, StitchRvsExec.stitch(rv, resVal, outputRvRange))
          case _ =>
            rhsMap.put(jk, rv)
        }
      }
    }

    // 4) Prepare result accumulator
    val result = new mutable.HashMap[Map[Utf8Str, Utf8Str], // join‐key
      mutable.Map[Map[Utf8Str, Utf8Str], RangeVector]]() // inner: full labelValues → RV
    val period = lhsRvs.headOption.flatMap(_.outputRange)

    // 5) For each LHS, either emit unchanged (if no RHS) or do the “unless”‐zip
    lhsRvs.foreach { lhs =>
      val jk = joinKeys(lhs.key)

      if (rhsMap.contains(jk)) {
        // Fetch (or create) the inner map for this join‐key
        val innerMap = result.getOrElse(jk, mutable.Map.empty)

        // Stitch any duplicate LHS series by full labelValues
        val prevRvOpt = innerMap.get(lhs.key.labelValues)
        val lhsStitched = prevRvOpt match {
          case Some(prevRv) =>
            StitchRvsExec.stitch(lhs, prevRv, outputRvRange)
          case None =>
            lhs
        }

        // build an “unless” cursor: NaN where RHS had any sample
        val lhsRows = lhsStitched.rows()
        val rhsRows = rhsMap(jk).rows()
        val cursor  = new RangeVectorCursor {
          private val cur = new TransientRow()
          override def hasNext: Boolean = lhsRows.hasNext
          override def next(): RowReader = {
            val l = lhsRows.next()
            val v =
              if (rhsRows.hasNext) {
                val r = rhsRows.next()
                if (!java.lang.Double.isNaN(r.getDouble(1))) Double.NaN
                else l.getDouble(1)
              } else l.getDouble(1)
            cur.setValues(l.getLong(0), v)
            cur
          }
          override def close(): Unit = {
            lhsRows.close()
            rhsRows.close()
          }
        }

        val outRv = IteratorBackedRangeVector(lhs.key, cursor, period)
        innerMap.put(lhs.key.labelValues, outRv)
        result.put(jk, innerMap)
      } else {
        // no RHS for this key ⇒ emit LHS (and stitch duplicates)
        val innerMap = result.getOrElseUpdate(jk, mutable.Map.empty)

        innerMap.get(lhs.key.labelValues) match {
          case Some(prevRv) =>
            val merged = StitchRvsExec.stitch(lhs, prevRv, outputRvRange)
            innerMap.put(lhs.key.labelValues, merged)
          case None =>
            innerMap.put(lhs.key.labelValues, lhs)
        }

        result.put(jk, innerMap)
      }
    }

    // 6) flatten all per-key buffers back into one Iterator
    result.valuesIterator.flatMap(_.valuesIterator)
  }
  // scalastyle:on method.length

}
