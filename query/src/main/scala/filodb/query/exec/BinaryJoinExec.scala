package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String => Utf8Str}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.exec.binaryOp.BinaryOperatorFunction

/**
  * Binary join operator between results of lhs and rhs plan.
  *
  * This ExecPlan accepts two sets of RangeVectors lhs and rhs from child plans.
  * It then does a join of the RangeVectors based on the fields of their keys as
  * dictated by `on` or `ignoring` fields passed as params.
  *
  * Joins can be one-to-one or one-to-many. One-to-One is currently supported using a hash based join.
  * One-to-Many is yet to be implemented. Many-to-Many is not supported for math based joins.
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
  */
final case class BinaryJoinExec(id: String,
                                dispatcher: PlanDispatcher,
                                lhs: Seq[ExecPlan],
                                rhs: Seq[ExecPlan],
                                binaryOp: BinaryOperator,
                                cardinality: Cardinality,
                                on: Seq[String],
                                ignoring: Seq[String]) extends NonLeafExecPlan {
  require(cardinality != Cardinality.ManyToMany || binaryOp.isInstanceOf[ComparisonOperator],
    "Many To Many cardinality is supported only for comparison binary operators")
  require(on == Nil || ignoring == Nil, "Cannot specify both 'on' and 'ignoring' clause")
  require(cardinality == Cardinality.OneToOne, "Currently only one-to-one cardinality is supported")
  require(!on.contains("__name__"), "On cannot contain metric name")

  val onLabels = on.map(Utf8Str(_)).toSet
  val ignoringLabels = ignoring.map(Utf8Str(_)).toSet + "__name__".utf8
  // if onLabels is non-empty, we are doing matching based on on-label, otherwise we are
  // doing matching based on ignoringLabels even if it is empty
  val onMatching = onLabels.nonEmpty

  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = lhs(0).schema(dataset)

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  protected[exec] def compose(childResponses: Observable[QueryResponse],
                              queryConfig: QueryConfig): Observable[RangeVector] = {
    val taskOfResults = childResponses.map {
      case QueryResult(_, _, result) => result
      case QueryError(_, ex)         => throw ex
    }.toListL.map { resp =>
      require(resp.size == lhs.size + rhs.size, "Did not get sufficient responses for LHS and RHS")
      val lhsRvs = resp.take(lhs.size).flatten
      val rhsRvs = resp.drop(lhs.size).flatten

      // figure out which side is the "one" side
      val (oneSide, otherSide, lhsIsOneSide) =
        if (cardinality == Cardinality.OneToMany) (lhsRvs, rhsRvs, true)
        else (rhsRvs, lhsRvs, false)

      // load "one" side keys in a hashmap
      val oneSideMap = new mutable.HashMap[Map[Utf8Str, Utf8Str], RangeVector]()
      oneSide.foreach { rv =>
        val jk = joinKeys(rv.key)
        if (oneSideMap.contains(jk))
          throw new BadQueryException(s"Cardinality $cardinality was used, but many found instead of one for $jk")
        oneSideMap.put(joinKeys(rv.key), rv)
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
          val res = if (lhsIsOneSide) binOp(rvOne.rows, rvOther.rows) else binOp(rvOther.rows, rvOne.rows)
          IteratorBackedRangeVector(resKey, res)
        }
      }
      Observable.fromIterable(results)
    }
    Observable.fromTask(taskOfResults).flatten
  }

  private def joinKeys(rvk: RangeVectorKey): Map[Utf8Str, Utf8Str] = {
    if (onLabels.nonEmpty) rvk.labelValues.filter(lv => onLabels.contains(lv._1))
    else rvk.labelValues.filterNot(lv => ignoringLabels.contains(lv._1))
  }

  private def resultKeys(oneSideKey: RangeVectorKey, otherSideKey: RangeVectorKey): RangeVectorKey = {
    // start from otherSideKey which could be many or one
    var result = otherSideKey.labelValues
    // drop metric name if math operator
    // TODO use dataset's metricName column name here instead of hard-coding column
    if (binaryOp.isInstanceOf[MathOperator]) result = result - Utf8Str("__name__")

    if (cardinality == Cardinality.OneToOne) {
      result = if (onLabels.nonEmpty) result.filter(lv => onLabels.contains(lv._1)) // retain what is in onLabel list
               else result.filterNot(lv => ignoringLabels.contains(lv._1)) // remove the labels in ignoring label list
    }
    // TODO handle one-to-many case
    CustomRangeVectorKey(result)
  }

  private def binOp(lhsRows: Iterator[RowReader], rhsRows: Iterator[RowReader]): Iterator[RowReader] = {
    new Iterator[RowReader] {
      val cur = new TransientRow()
      val binFunc = BinaryOperatorFunction.factoryMethod(binaryOp)
      override def hasNext: Boolean = lhsRows.hasNext && rhsRows.hasNext
      override def next(): RowReader = {
        val lhsRow = lhsRows.next()
        val rhsRow = rhsRows.next()
        cur.setValues(lhsRow.getLong(0), binFunc.calculate(lhsRow.getDouble(1), rhsRow.getDouble(1)))
        cur
      }
    }
  }
}

