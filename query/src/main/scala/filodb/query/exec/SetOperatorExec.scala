package filodb.query.exec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.memstore.SchemaMismatch
import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => Utf8Str}
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
                                 metricColumn: String) extends NonLeafExecPlan {
  require(on == Nil || ignoring == Nil, "Cannot specify both 'on' and 'ignoring' clause")
  require(!on.contains(metricColumn), "On cannot contain metric name")

  val onLabels = on.map(Utf8Str(_)).toSet
  // TODO Add unit tests for automatic inclusion of _pi_ and _step_ in the join key
  val withExtraOnLabels = onLabels ++ Seq("_pi_".utf8, "_step_".utf8)
  val ignoringLabels = ignoring.map(Utf8Str(_)).toSet + metricColumn.utf8
  // if onLabels is non-empty, we are doing matching based on on-label, otherwise we are
  // doing matching based on ignoringLabels even if it is empty

  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  protected[exec] def compose(childResponses: Observable[(QueryResponse, Int)],
                              firstSchema: Task[ResultSchema],
                              querySession: QuerySession): Observable[RangeVector] = {
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result), i) => (result, i)
      case (QueryError(_, ex), _)         => throw ex
    }.toListL.map { resp =>
      // NOTE: We can't require this any more, as multischema queries may result in not a QueryResult if the
      //       filter returns empty results.  The reason is that the schema will be undefined.
      // require(resp.size == lhs.size + rhs.size, "Did not get sufficient responses for LHS and RHS")
      val lhsRvs = resp.filter(_._2 < lhs.size).flatMap(_._1)
      val rhsRvs = resp.filter(_._2 >= lhs.size).flatMap(_._1)

      val results: List[RangeVector] = binaryOp  match {
        case LAND => setOpAnd(lhsRvs, rhsRvs)
        case LOR => setOpOr(lhsRvs, rhsRvs)
        case LUnless => setOpUnless(lhsRvs, rhsRvs)
        case _ => throw new IllegalArgumentException("requirement failed: " + "Only and, or and unless are supported ")
      }

      Observable.fromIterable(results)
    }
    Observable.fromTask(taskOfResults).flatten
  }

  private def joinKeys(rvk: RangeVectorKey): Map[Utf8Str, Utf8Str] = {
    if (onLabels.nonEmpty) rvk.labelValues.filter(lv => withExtraOnLabels.contains(lv._1))
    else rvk.labelValues.filterNot(lv => ignoringLabels.contains(lv._1))
  }

  private def setOpAnd(lhsRvs: List[RangeVector]
                       , rhsRvs: List[RangeVector]): List[RangeVector] = {
    val rhsKeysSet = new mutable.HashSet[Map[Utf8Str, Utf8Str]]()
    var result = new ListBuffer[RangeVector]()
    rhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      if (jk.nonEmpty)
        rhsKeysSet += jk
    }

    lhsRvs.foreach { lhs =>
      val jk = joinKeys(lhs.key)
      // Add range vectors from lhs which are present in lhs and rhs both
      // Result should also have range vectors for which rhs does not have any keys
      if (rhsKeysSet.contains(jk) || rhsKeysSet.isEmpty) {
        result += lhs
      }
    }
    result.toList
  }

  private def setOpOr(lhsRvs: List[RangeVector]
                      , rhsRvs: List[RangeVector]): List[RangeVector] = {
    val lhsKeysSet = new mutable.HashSet[Map[Utf8Str, Utf8Str]]()
    var result = new ListBuffer[RangeVector]()
    // Add everything from left hand side range vector
    lhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      lhsKeysSet += jk
      result += rv
    }
    // Add range vectors from right hand side which are not present on lhs
    rhsRvs.foreach { rhs =>
      val jk = joinKeys(rhs.key)
      if (!lhsKeysSet.contains(jk)) {
        result += rhs
      }
    }
    result.toList
  }

  private def setOpUnless(lhsRvs: List[RangeVector]
                          , rhsRvs: List[RangeVector]): List[RangeVector] = {
    val rhsKeysSet = new mutable.HashSet[Map[Utf8Str, Utf8Str]]()
    var result = new ListBuffer[RangeVector]()
    rhsRvs.foreach { rv =>
      val jk = joinKeys(rv.key)
      rhsKeysSet += jk
    }
    // Add range vectors which are not present in rhs
    lhsRvs.foreach { lhs =>
      val jk = joinKeys(lhs.key)
      if (!rhsKeysSet.contains(jk)) {
        result += lhs
      }
    }
    result.toList
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
