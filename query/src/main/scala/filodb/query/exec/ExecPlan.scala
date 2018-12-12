package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.metadata.Dataset
import filodb.core.query.{RangeVector, ResultSchema, SerializableRangeVector}
import filodb.core.store.ChunkSource
import filodb.memory.format.RowReader
import filodb.query._
import filodb.query.Query.qLogger

/**
  * This is the Execution Plan tree node interface.
  * ExecPlan nodes form a tree. Each leaf node in the tree
  * translates to a data extraction sub-query. Execution of
  * non-leaf nodes triggers a scatter-gather operation followed
  * by composition of results. Root node returns the result
  * of the entire query.
  *
  * Association to RangeVectorTransformer objects allow each node
  * to perform data transformations closer to the
  * source node and minimize data movement over the wire.
  *
  * Convention is for all concrete subclasses of ExecPlan to
  * end with 'Exec' for easy identification
  */
trait ExecPlan extends QueryCommand {
  /**
    * The query id
    */
  def id: String

  /**
    * Child execution plans representing sub-queries
    */
  def children: Seq[ExecPlan]

  def submitTime: Long

  /**
    * Limit on number of samples returned per RangeVector
    */
  def limit: Int

  def dataset: DatasetRef

  /**
    * The dispatcher is used to dispatch the ExecPlan
    * to the node where it will be executed. The Query Engine
    * will supply this parameter
    */
  def dispatcher: PlanDispatcher

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
    * Facade for the execution orchestration of the plan sub-tree
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
      val finalRes = rangeVectorTransformers.foldLeft((res, schema)) { (acc, transf) =>
        qLogger.debug(s"queryId: ${id} Started Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
        (transf.apply(acc._1, queryConfig, limit, acc._2), transf.schema(dataset, acc._2))
      }
      val recSchema = SerializableRangeVector.toSchema(finalRes._2.columns, finalRes._2.brSchemas)
      val builder = SerializableRangeVector.toBuilder(recSchema)
      finalRes._1
        .map {
          case r: SerializableRangeVector => r
          case rv: RangeVector =>
            // materialize, and limit rows per RV
            SerializableRangeVector(rv, builder, recSchema, limit)
        }
        .take(queryConfig.vectorsLimit)
        .toListL
        .map { r =>
          val numBytes = builder.allContainers.map(_.numBytes).sum
          SerializableRangeVector.queryResultBytes.record(numBytes)
          if (numBytes > 5000000) {
            // 5MB limit. Configure if necessary later.
            // 250 RVs * (250 bytes for RV-Key + 200 samples * 32 bytes per sample)
            // is < 2MB
            qLogger.warn(s"queryId: ${id} result was " +
              s"large size ${numBytes}. May need to tweak limits. " +
              s"ExecPlan was: ${printTree()} " +
              s"Limit was: ${limit}")
          }
          qLogger.debug(s"queryId: ${id} Successful execution of ${getClass.getSimpleName} with transformers")
          QueryResult(id, finalRes._2, r)
        }
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
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  protected def schemaOfDoExecute(dataset: Dataset): ResultSchema

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  protected def args: String

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

  protected def rowIterAccumulator(srvsList: List[Seq[SerializableRangeVector]]): Iterator[RowReader] = {

    new Iterator[RowReader] {
      val listSize = srvsList.size
      val rowIteratorList = srvsList.map(srvs => srvs(0).rows)
      private var curIterIndex = 0
      override def hasNext: Boolean = rowIteratorList(curIterIndex).hasNext ||
        (curIterIndex < listSize - 1
          && (rowIteratorList({curIterIndex += 1; curIterIndex}).hasNext || this.hasNext)) // find non empty iterator

      override def next(): RowReader = rowIteratorList(curIterIndex).next()
    }
  }
}

abstract class LeafExecPlan extends ExecPlan {
  final def children: Seq[ExecPlan] = Nil
}

abstract class NonLeafExecPlan extends ExecPlan {

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

  final protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = schemaOfCompose(dataset)

  /**
    * Schema of the RangeVectors returned by compose() method
    */
  protected def schemaOfCompose(dataset: Dataset): ResultSchema

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the sub-query results here.
    */
  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector]

}
