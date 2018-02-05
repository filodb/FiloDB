package filodb.coordinator.client

import QueryCommands._

import filodb.core.query.CombinerFunction

/**
 * A LogicalPlan describes a query without specific sharding/clustering/execution details.  This is a public API.
 */
sealed trait LogicalPlan {
  def children: Seq[LogicalPlan]
}

/**
 * NOTE: These concepts mirror the ones in org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
 */
trait LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
}

abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil
}


object LogicalPlan {
  /**
   * Create a simple LogicalPlan consisting of aggregating(reducing) each partition into a Tuple, then
   * combining Tuples from diff partitions into a single one.
   * @param childPlan the underlying LogicalPlan which should return a set of vectors
   */
  def simpleAgg(aggFunc: String, aggArgs: Seq[String] = Nil,
                combFunc: String = CombinerFunction.default, combArgs: Seq[String] = Nil,
                childPlan: LeafNode): LogicalPlan =
    ReducePartitions(combFunc, combArgs, ReduceEach(aggFunc, aggArgs, childPlan))

  /**
   * Leaf node, the latest sample of multiple partitions specified using a PartitionQuery
   * @param partQuery specifies what partitions or time series to fetch
   * @param columns the data column(s) to fetch.  Row key columns such as "timestamp" do not need to be specified here.
   *                Thus most of the time this will just be one column.
   */
  final case class PartitionsInstant(partQuery: PartitionQuery,
                                     columns: Seq[String]) extends LeafNode

  /**
   * Leaf node, a selected range of multiple partitions
   * @param partQuery specifies what partitions or time series to fetch
   * @param dataQuery specifies what range of values in each partition/time series to operate on
   * @param columns the data column(s) to fetch.  Row key columns such as "timestamp" do not need to be specified here.
   *                Thus most of the time this will just be one column.
   */
  final case class PartitionsRange(partQuery: PartitionQuery,
                                   dataQuery: DataQuery,
                                   columns: Seq[String]) extends LeafNode

  object PartitionsRange {
    // A shortcut for all the samples in a time series
    def all(partQuery: PartitionQuery, columns: Seq[String]): PartitionsRange =
      PartitionsRange(partQuery, AllPartitionData, columns)
  }

  /**
   * ****  These are not leaf nodes, so they have dependencies on leaf nodes
   */

  /**
   * Reduce across partitions.  Can in theory be combined with other Aggregate* or leaf nodes.
   */
  final case class ReducePartitions(funcName: String, args: Seq[String], child: LogicalPlan) extends UnaryNode

  /**
   * Reduce each partition vector into a Tuple.  Should probably only have a leaf node as the next one.
   */
  final case class ReduceEach(funcName: String, args: Seq[String], child: LogicalPlan) extends UnaryNode
}


