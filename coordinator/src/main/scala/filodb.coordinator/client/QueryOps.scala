package filodb.coordinator.client

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

import filodb.core._
import filodb.query.{LogicalPlan => LogicalPlan2}

trait QueryOps extends ClientBase with StrictLogging {
  import QueryCommands._

  /**
   * Retrieves all the tags or columns from the MemStore that are currently indexed.
   * @param dataset the Dataset (and Database) to query
   * @param limit   the maximum number of results to return
   * @param timeout the maximum amount of time to wait for an answer
   * @return a Seq[String] with all the tag names, Nil if nothing is indexed or dataset not found
   */
  def getIndexNames(dataset: DatasetRef,
                    limit: Int = 10,
                    timeout: FiniteDuration = 15.seconds): Seq[String] =
    askCoordinator(GetIndexNames(dataset, limit, System.currentTimeMillis()), timeout) {
      case s: Seq[String] @unchecked => s
    }

  /**
   * Returns a Seq[String] of the first *limit* values indexed for a given tag/column.
   * @param dataset the Dataset (and Database) to query
   * @param indexName the name of the index to get values for
   * @param limit   the maximum number of results to return
   * @param timeout the maximum amount of time to wait for an answer
   * @return a Seq[String] with all the tag names, Nil if nothing is indexed or dataset not found
   */
  def getIndexValues(dataset: DatasetRef,
                     indexName: String,
                     limit: Int = 100,
                     timeout: FiniteDuration = 15.seconds): Seq[String] =
    askCoordinator(GetIndexValues(dataset, indexName, limit, System.currentTimeMillis()), timeout) {
      case s: Seq[String] @unchecked => s
    }

  /**
   * Asks the FiloDB node to perform a query using a LogicalPlan.
   * @param dataset the Dataset (and Database) to query
   * @param plan the query LogicalPlan to execute
   * @param options the query options including spread and inter-node query timeout.
   *        NOTE: the actual response timeout is longer as we need to allow time for errors to propagagte back.
   */
  def logicalPlanQuery(dataset: DatasetRef,
                       plan: LogicalPlan,
                       options: QueryOptions = QueryOptions()): QueryResult = {
    val qCmd = LogicalPlanQuery(dataset, plan, options, System.currentTimeMillis())
    // NOTE: It's very important to extend the query timeout for the ask itself, because the queryTimeoutSecs is
    // the internal FiloDB scatter-gather timeout.  We need additional time for the proper error to get transmitted
    // back in case of internal timeouts.
    askCoordinator(qCmd, (options.queryTimeoutSecs + 10).seconds) { case r: QueryResult => r }
  }


  def logicalPlan2Query(dataset: DatasetRef,
                       plan: LogicalPlan2,
                       options: QueryOptions = QueryOptions()): QueryResult = {
    val qCmd = LogicalPlan2Query(dataset, plan, options)
    // NOTE: It's very important to extend the query timeout for the ask itself, because the queryTimeoutSecs is
    // the internal FiloDB scatter-gather timeout.  We need additional time for the proper error to get transmitted
    // back in case of internal timeouts.
    askCoordinator(qCmd, (options.queryTimeoutSecs + 10).seconds) { case r: QueryResult => r }
  }

}