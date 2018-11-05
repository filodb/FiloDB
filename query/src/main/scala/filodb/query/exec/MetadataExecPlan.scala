package filodb.query.exec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
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
trait MetadataExecPlan extends RootExecPlan {

  /**
    * The list of RangeVector transformations that will be done
    * after the doExecute method results are obtained. This
    * can be used to perform data transformations closer to the
    * source node and minimize data movement over the wire.
    */
  val rangeVectorTransformers = new ArrayBuffer[RangeVectorTransformer]()

  final def addRangeVectorTransformer(mapper: RangeVectorTransformer): Unit = {
    rangeVectorTransformers += mapper
  }

  /**
    * Schema of QueryResponse returned by running execute()
    */
  final def schema(dataset: Dataset): ResultSchema = {
    val source = schemaOfDoExecute(dataset)
    rangeVectorTransformers.foldLeft(source) { (acc, transf) => transf.schema(dataset, acc) }
  }

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
      qLogger.debug(s"queryId: ${id} Started ExecPlan ${getClass.getSimpleName} with $args")
      val res = doExecute(source, dataset, queryConfig)
      val schema = schemaOfDoExecute(dataset)
      res
        .toListL
        .map(r => {
          MetadataQueryResult(id, schema, r)
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
    val transf = rangeVectorTransformers.reverse.zipWithIndex.map { case (t, i) =>
      s"${"-"*(level + i)}T~${t.getClass.getSimpleName}(${t.args})"
    }
    val nextLevel = rangeVectorTransformers.size + level
    val curNode = s"${"-"*nextLevel}E~${getClass.getSimpleName}($args) on ${dispatcher}"
    val childr = children.map(_.printTree(nextLevel + 1))
    ((transf :+ curNode) ++ childr).mkString("\n")
  }
}

final case class NonLeafMetadataExecPlan(id: String,
                                         dispatcher: PlanDispatcher,
                                         children: Seq[RootExecPlan]) extends MetadataExecPlan {

  require(!children.isEmpty)

  protected def args: String = ""

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = children.head.schema(dataset)

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
    var responses = new ArrayBuffer[RangeVector]()
    val taskOfResults = childResponses.map {
      case MetadataQueryResult(_, _, result) => result
      case QueryError(_, ex)         => throw ex
    }.toListL.map { resp =>
      val rowKey = resp.head.head.key
      val resultMetadata = new mutable.HashSet[Map[UTF8Str, UTF8Str]]()
      resp.flatten.foreach(rv => {
        rv match {
          case MetadataRangeVector(_, metadata) => resultMetadata ++= metadata
        }
      })
      MetadataRangeVector(rowKey, resultMetadata)
    }
    Observable.fromTask(taskOfResults)
  }

  final protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = schemaOfCompose(dataset)

}


final case class  MetadataExecLeafPlan(id: String,
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
      val response = memStore.metadata(dataset, shard, filters, columns, end, start)
      val keysMap = Map(UTF8Str("ignore") -> UTF8Str("ignore"))
      Observable(MetadataRangeVector(CustomRangeVectorKey(keysMap), response))
    } else {
      Observable.empty
    }
  }

  protected def args: String = s"shard=$shard, filters=$filters"

  /**
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  override protected def schemaOfDoExecute(dataset: Dataset): ResultSchema =
    ResultSchema(Seq(ColumnInfo("ignore", ColumnType.MapColumn)), 1)

}
