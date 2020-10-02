package filodb.coordinator.client

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

import filodb.core._
import filodb.core.query.QueryContext
import filodb.query.{LogicalPlan => LogicalPlan2, QueryResponse => QueryResponse2}

trait QueryOps extends ClientBase with StrictLogging {
  import QueryCommands._

  /**
   * Retrieves all the tags or columns from the MemStore that are currently indexed.
   * @param dataset the Dataset (and Database) to query
   * @param limit   the maximum number of results to return
   * @param timeout the maximum amount of time to wait for an answer
   * @return a Set[String] with all the tag names, Nil if nothing is indexed or dataset not found
   */
  def getIndexNames(dataset: DatasetRef,
                    limit: Int = 10,
                    timeout: FiniteDuration = 15.seconds): Set[String] =
    askCoordinator(GetIndexNames(dataset, limit, System.currentTimeMillis()), timeout) {
      case s: Seq[String] @unchecked => s.toSet
    }

  /**
   * Returns a Seq[(String, Int)] of the top *limit* most popular values indexed for a given tag/column.
   * @param dataset the Dataset (and Database) to query
   * @param indexName the name of the index to get values for
   * @param shard the shard to query index values for
   * @param limit   the maximum number of results to return
   * @param timeout the maximum amount of time to wait for an answer
   * @return a Seq[(String, Int)] with all the tag names, Nil if nothing is indexed or dataset not found
   *        each tag followed by frequency, in descending order of frequency
   */
  def getIndexValues(dataset: DatasetRef,
                     indexName: String,
                     shard: Int,
                     limit: Int = 100,
                     timeout: FiniteDuration = 15.seconds): Seq[(String, Int)] =
    askCoordinator(GetIndexValues(dataset, indexName, shard, limit, System.currentTimeMillis()), timeout) {
      case s: Seq[(String, Int)] @unchecked => s
    }

  /**
    * Asks the FiloDB node to perform a query using a LogicalPlan.
    * @param dataset the Dataset (and Database) to query
    * @param plan the query LogicalPlan to execute
    * @param qContext the query options including spread and inter-node query timeout.
    *        NOTE: the actual response timeout is longer as we need to allow time for errors to propagagte back.
    */
  def logicalPlan2Query(dataset: DatasetRef,
                       plan: LogicalPlan2,
                        qContext: QueryContext = QueryContext()): QueryResponse2 = {
    val qCmd = LogicalPlan2Query(dataset, plan, qContext)
    // NOTE: It's very important to extend the query timeout for the ask itself, because the queryTimeoutMillis is
    // the internal FiloDB scatter-gather timeout.  We need additional time for the proper error to get transmitted
    // back in case of internal timeouts.
    askCoordinator(qCmd, (qContext.plannerParam.queryTimeoutMillis + 10000).millis) { case r: QueryResponse2 => r }
  }

}