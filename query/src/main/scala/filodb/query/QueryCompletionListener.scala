package filodb.query

import filodb.core.query.{QueryContext, QueryStats}

/**
 * Receives a callback on query completion.
 * Intended primarily for processing QueryStats.
 */
trait QueryCompletionListener {
  /**
   * @param queryContext context of the query that was executed.
   * @param queryStats   stats produced while executing the query.
   */
  def onQueryComplete(queryContext: QueryContext, queryStats: QueryStats): Unit
}

object QueryCompletionListener {
  /** Listener that does nothing. Default for servers that do not need completion callbacks. */
  val NoOp: QueryCompletionListener = new QueryCompletionListener {
    override def onQueryComplete(queryContext: QueryContext, queryStats: QueryStats): Unit = {}
  }
}
