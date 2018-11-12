package filodb.query.exec

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.MemStore
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.query._
import filodb.query.Query.qLogger

/**
  * Metadata query execution plan
  */
trait MetadataExecPlan extends BaseExecPlan {


  /**
    * Schema of QueryResponse returned by running execute()
    */
  def schema(dataset: Dataset): ResultSchema = ???

  /**
    * Facade for the metadata query execution orchestration of the plan sub-tree
    * starting from this node.
    *
    * The return Task must be "run" for execution to ensue. See
    * Monix documentation for further information on Task.
    * This first invokes the doExecute abstract method, then applies
    * the RangeVectorMappers associated with this plan node.
    *
    * The returned task can be used to perform post-execution steps
    * such as sending off an asynchronous response message etc.
    *
    */
  final def execute(source: ChunkSource,
                    dataset: Dataset,
                    queryConfig: QueryConfig)
                   (implicit sched: Scheduler,
                    timeout: FiniteDuration): Task[QueryResponse] = {
    try {
      qLogger.debug(s"queryId: ${id} Started ExecPlan ${getClass.getSimpleName}")
      val res = doExecute(source, dataset, queryConfig)
      res
        .firstL
        .map(r => {
          MetadataQueryResult(id, r)
        })
      .onErrorHandle { case ex: Throwable =>
        qLogger.error(s"queryId: ${id} Exception during execution of query: ${printTree()}", ex)
        QueryError(id, ex)
      }
    } catch { case NonFatal(ex) =>
      qLogger.error(s"queryId: ${id} Exception during orchestration of query: ${printTree()}", ex)
      Task(QueryError(id, ex))
    }
  }

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector]

  /**
    * Prints the ExecPlan and RangeVectorTransformer execution flow as a tree
    * structure, useful for debugging
    */
  final def printTree(level: Int = 0): String = {
    val nextLevel = level
    val curNode = s"${"-"*nextLevel}E~${getClass.getSimpleName} on ${dispatcher}"
    val childr = children.map(_.printTree(nextLevel + 1))
    (curNode ++ childr).mkString("\n")
  }
}

final case class NonLeafMetadataExecPlan(id: String,
                                         dispatcher: PlanDispatcher,
                                         children: Seq[BaseExecPlan]) extends MetadataExecPlan {

  require(!children.isEmpty)

  /**
    * For now we do not support cross-dataset queries
    */
  final def dataset: DatasetRef = children.head.dataset

  final def submitTime: Long = children.head.submitTime

  final def limit: Int = children.head.limit

  /**
    * Being a non-leaf node, this implementation encompasses the logic
    * of child plan execution. It then composes the sub-query results
    * using the abstract method 'compose' to arrive at the higher level
    * result
    */
  final protected def doExecute(source: ChunkSource,
                                dataset: Dataset,
                                queryConfig: QueryConfig)
                               (implicit sched: Scheduler,
                                timeout: FiniteDuration): Observable[RangeVector] = {
    val childTasks = Observable.fromIterable(children).mapAsync { plan =>
      plan.dispatcher.dispatch(plan).onErrorHandle { case ex: Throwable =>
        qLogger.error(s"queryId: ${id} Execution failed for sub-query ${plan.printTree()}", ex)
        QueryError(id, ex)
      }
    }
    compose(childTasks, queryConfig)
  }

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the sub-query results here.
    */
  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig)(implicit sched: Scheduler,
                                                  timeout: FiniteDuration): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case MetadataQueryResult(_, result) => result
      case QueryError(_, ex)         => throw ex
    }.toListL.map { resp =>
      var metadataResult = Seq.empty[UTF8Str]
      // extract record schema from the first record - since schema is same for all the leaf results
      val schema = resp.head match {
        case RecordList(records, schema) => schema
      }
      resp.foreach(rv => {
        rv match {
          case RecordList(records, _) => metadataResult ++= records
        }
      })
      RecordList(metadataResult.distinct.toList, schema) //distinct -> result may have duplicates in case of labelValues
    }
    Observable.fromTask(taskOfResults)
  }

}


final case class  SeriesKeyExecLeafPlan(id: String,
                                        submitTime: Long,
                                        limit: Int,
                                        dispatcher: PlanDispatcher,
                                        dataset: DatasetRef,
                                        shard: Int,
                                        filters: Seq[ColumnFilter],
                                        start: Long,
                                        end: Long,
                                        columns: Seq[String]) extends MetadataExecPlan {

  final def children: Seq[ExecPlan] = Nil

  protected def doExecute(source: ChunkSource,
                          dataset1: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {

    if (source.isInstanceOf[MemStore]) {
      var memStore = source.asInstanceOf[MemStore]
      val response = memStore.indexValuesWithFilters(dataset, shard, filters, Option.empty, end, start)
      Observable(RecordList(response, new RecordSchema(Seq(ColumnType.MapColumn))))
    } else {
      Observable.empty
    }
  }
}

final case class  LabelValuesExecLeafPlan(id: String,
                                        submitTime: Long,
                                        limit: Int,
                                        dispatcher: PlanDispatcher,
                                        dataset: DatasetRef,
                                        shard: Int,
                                        filters: Seq[ColumnFilter],
                                        column: String) extends MetadataExecPlan {

  final def lookBackInMillis: Long = 86400000 // (24hrs) TODO get from configuration
  final def children: Seq[ExecPlan] = Nil

  protected def doExecute(source: ChunkSource,
                          dataset1: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {

    if (source.isInstanceOf[MemStore]) {
      var memStore = source.asInstanceOf[MemStore]
      val curr = System.currentTimeMillis()
      val end = curr - curr % 1000 // round to the floor second
      val start = end - lookBackInMillis
      val response = filters.isEmpty match {
        case true => memStore.indexValues(dataset, shard, column).map(_.term).toList
        case false => memStore.indexValuesWithFilters(dataset, shard, filters, Option(column), end, start)
      }
      Observable(RecordList(response, new RecordSchema(Seq(ColumnType.StringColumn))))
    } else {
      Observable.empty
    }
  }

}
