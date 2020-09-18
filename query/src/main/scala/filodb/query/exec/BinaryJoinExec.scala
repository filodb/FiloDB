package filodb.query.exec

import scala.collection.mutable

import kamon.Kamon
import monix.eval.Task
import monix.reactive.Observable

import filodb.core.memstore.SchemaMismatch
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => Utf8Str}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.binaryOp.BinaryOperatorFunction

/**
  * Binary join operator between results of lhs and rhs plan.
  *
  * This ExecPlan accepts two sets of RangeVectors lhs and rhs from child plans.
  * It then does a join of the RangeVectors based on the fields of their keys as
  * dictated by `on` or `ignoring` fields passed as params.
  *
  * Joins can be one-to-one or one-to-many. One-to-One is currently supported using a hash based join.
  *
  * The performance is going to be not-so-optimal since it will involve moving possibly lots of matching range vector
  * data across machines. Histogram based joins can and will be optimized by co-location of bucket, count and sum
  * data and will be joined close to the source using another "HistogramBinaryJoinExec" exec plan.
  *
  * @param lhs ExecPlan that will return results of LHS expression
  * @param rhs ExecPlan that will return results of RHS expression
  * @param binaryOp the binary operator
  * @param cardinality the cardinality of the join relationship as a hint
  * @param on fields from range vector keys to include while performing the join
  * @param ignoring fields from range vector keys to exclude while performing the join
  * @param include labels specified in group_left/group_right to be included from one side
  */
final case class BinaryJoinExec(queryContext: QueryContext,
                                dispatcher: PlanDispatcher,
                                lhs: Seq[ExecPlan],
                                rhs: Seq[ExecPlan],
                                binaryOp: BinaryOperator,
                                cardinality: Cardinality,
                                on: Seq[String],
                                ignoring: Seq[String],
                                include: Seq[String],
                                metricColumn: String) extends NonLeafExecPlan {

  require(cardinality != Cardinality.ManyToMany,
    "Many To Many cardinality is not supported for BinaryJoinExec")
  require(on == Nil || ignoring == Nil, "Cannot specify both 'on' and 'ignoring' clause")
  require(!on.contains(metricColumn), "On cannot contain metric name")

  val onLabels = on.map(Utf8Str(_)).toSet
  // publishInterval and step tags always needs to be included in join key
  val withExtraOnLabels = onLabels ++ Seq("_pi_".utf8, "_step_".utf8)
  val ignoringLabels = ignoring.map(Utf8Str(_)).toSet
  val ignoringLabelsForJoin = ignoringLabels + metricColumn.utf8
  // if onLabels is non-empty, we are doing matching based on on-label, otherwise we are
  // doing matching based on ignoringLabels even if it is empty

  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  //scalastyle:off method.length
  protected[exec] def compose(childResponses: Observable[(QueryResponse, Int)],
                              firstSchema: Task[ResultSchema],
                              querySession: QuerySession): Observable[RangeVector] = {
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result), _)
        if (result.size  > queryContext.joinQueryCardLimit && cardinality == Cardinality.OneToOne) =>
        throw new BadQueryException(s"This query results in more than ${queryContext.joinQueryCardLimit} " +
          s"join cardinality. Try applying more filters.")
      case (QueryResult(_, _, result), i) => (result, i)
      case (QueryError(_, ex), _)         => throw ex
    }.toListL.map { resp =>
      Kamon.histogram("query-execute-time-elapsed-step2-child-results-available")
        .withTag("plan", getClass.getSimpleName)
        .record(System.currentTimeMillis - queryContext.submitTime)
      // NOTE: We can't require this any more, as multischema queries may result in not a QueryResult if the
      //       filter returns empty results.  The reason is that the schema will be undefined.
      // require(resp.size == lhs.size + rhs.size, "Did not get sufficient responses for LHS and RHS")
      val lhsRvs = resp.filter(_._2 < lhs.size).flatMap(_._1)
      val rhsRvs = resp.filter(_._2 >= lhs.size).flatMap(_._1)
      // figure out which side is the "one" side
      val (oneSide, otherSide, lhsIsOneSide) =
        if (cardinality == Cardinality.OneToMany) (lhsRvs, rhsRvs, true)
        else (rhsRvs, lhsRvs, false)
      // load "one" side keys in a hashmap
      val oneSideMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()
      oneSide.foreach { rv =>
        val jk = joinKeys(rv.key)
        if (oneSideMap.contains(jk)) {
          qLogger.info(s"BinaryJoinError: RV ${rv.key} (IDs ${rv.key.partIds}) produced $jk, but already in map: " +
                      s"RV ${oneSideMap(jk).key} (IDs ${oneSideMap(jk).key.partIds})")
          throw new BadQueryException(s"Cardinality $cardinality was used, but many found instead of one for $jk")
        }
        oneSideMap.put(jk, rv)
      }
      // keep a hashset of result range vector keys to help ensure uniqueness of result range vectors
      val resultKeySet = new mutable.HashSet[RangeVectorKey]()
      // iterate across the the "other" side which could be one or many and perform the binary operation
      val results = otherSide.flatMap { rvOther =>
        val jk = joinKeys(rvOther.key)
        oneSideMap.get(jk).map { rvOne =>
          val resKey = resultKeys(rvOne.key, rvOther.key)
          if (resultKeySet.contains(resKey))
            throw new BadQueryException(s"Non-unique result vectors found for $resKey. " +
              s"Use grouping to create unique matching")
          resultKeySet.add(resKey)

          // OneToOne cardinality case is already handled. this condition handles OneToMany case
          if (resultKeySet.size > queryContext.joinQueryCardLimit)
            throw new BadQueryException(s"This query results in more than ${queryContext.joinQueryCardLimit} " +
              s"join cardinality. Try applying more filters.")

          val res = if (lhsIsOneSide) binOp(rvOne.rows, rvOther.rows) else binOp(rvOther.rows, rvOne.rows)
          IteratorBackedRangeVector(resKey, res)
        }
      }
      Observable.fromIterable(results)
    }
    Observable.fromTask(taskOfResults).flatten
  }

  private def joinKeys(rvk: RangeVectorKey): Map[Utf8Str, Utf8Str] = {
    if (onLabels.nonEmpty) rvk.labelValues.filter(lv => withExtraOnLabels.contains(lv._1))
    else rvk.labelValues.filterNot(lv => ignoringLabelsForJoin.contains(lv._1))
  }

  private def resultKeys(oneSideKey: RangeVectorKey, otherSideKey: RangeVectorKey): RangeVectorKey = {
    // start from otherSideKey which could be many or one
    var result = otherSideKey.labelValues
    // drop metric name if math operator
    if (binaryOp.isInstanceOf[MathOperator]) result = result - Utf8Str(metricColumn)

    if (cardinality == Cardinality.OneToOne) {
      result =
        if (onLabels.nonEmpty) result.filter(lv => withExtraOnLabels.contains(lv._1)) // retain what is in onLabel list
        else result.filterNot(lv => ignoringLabels.contains(lv._1)) // remove the labels in ignoring label list
    } else if (cardinality == Cardinality.OneToMany || cardinality == Cardinality.ManyToOne) {
      // For group_left/group_right add labels in include from one side. Result should have all keys from many side
      include.foreach { x =>
          val labelVal = oneSideKey.labelValues.get(Utf8Str(x))
          labelVal.foreach { v =>
            if (v.toString.equals(""))
              // If label value is empty do not propagate to result and
              // also delete from result
              result -= Utf8Str(x)
            else
              result += (Utf8Str(x) -> v)
          }
      }
    }
    CustomRangeVectorKey(result)
  }

  private def binOp(lhsRows: RangeVectorCursor,
                    rhsRows: RangeVectorCursor): RangeVectorCursor = {
    new RangeVectorCursor {
      val cur = new TransientRow()
      val binFunc = BinaryOperatorFunction.factoryMethod(binaryOp)
      override def hasNext: Boolean = lhsRows.hasNext && rhsRows.hasNext
      override def next(): RowReader = {
        val lhsRow = lhsRows.next()
        val rhsRow = rhsRows.next()
        cur.setValues(lhsRow.getLong(0), binFunc.calculate(lhsRow.getDouble(1), rhsRow.getDouble(1)))
        cur
      }

      override def close(): Unit = {
        lhsRows.close()
        rhsRows.close()
      }
    }
  }

  /**
    * overridden to allow schemas with different vector lengths, colids as long as the columns are same - to handle
    * binary joins between scalar/rangevectors
    */
  override def reduceSchemas(rs: ResultSchema, resp: QueryResult): ResultSchema = {
    resp match {
      case QueryResult(_, schema, _) if rs == ResultSchema.empty =>
        schema     /// First schema, take as is
      case QueryResult(_, schema, _) =>
        if (!rs.hasSameColumnsAs(schema)) throw SchemaMismatch(rs.toString, schema.toString)
        else rs
    }
  }
}

