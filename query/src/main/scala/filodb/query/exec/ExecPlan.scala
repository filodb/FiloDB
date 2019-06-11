package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import kamon.Kamon
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
    * Limit on number of samples returned by this ExecPlan
    */
  def limit: Int

  /**
    * Throw error if the size of the resultset is greater than Limit
    * Take first n (limit) elements if the flag is false. Applicable for Metadata Queries
    */
  def enforceLimit: Boolean = true

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
  // scalastyle:off method.length
  final def execute(source: ChunkSource,
                    dataset: Dataset,
                    queryConfig: QueryConfig)
                   (implicit sched: Scheduler,
                    timeout: FiniteDuration): Task[QueryResponse] = {
    // NOTE: we launch the preparatory steps as a Task too.  This is important because scanPartitions,
    // Lucene index lookup, and On-Demand Paging orchestration work could suck up nontrivial time and
    // we don't want these to happen in a single thread.
    Task {
      qLogger.debug(s"queryId: ${id} Setting up ExecPlan ${getClass.getSimpleName} with $args")
      val res = doExecute(source, dataset, queryConfig)
      val schema = schemaOfDoExecute(dataset)
      val res2 = rangeVectorTransformers.foldLeft((res, schema)) { (acc, transf) =>
        qLogger.debug(s"queryId: ${id} Setting up Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
        (transf.apply(acc._1, queryConfig, limit, acc._2), transf.schema(dataset, acc._2))
      }
      val recSchema = SerializableRangeVector.toSchema(res2._2.columns, res2._2.brSchemas)
      val builder = SerializableRangeVector.toBuilder(recSchema)
      var numResultSamples = 0 // BEWARE - do not modify concurrently!!
      qLogger.debug(s"queryId: ${id} Materializing SRVs from iterators if necessary")
      var finalRes = res2._1
        .map {
          case srv: SerializableRangeVector =>
            numResultSamples += srv.numRows
            // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
            if (enforceLimit && numResultSamples > limit)
              throw new BadQueryException(s"This query results in more than $limit samples. " +
                s"Try applying more filters or reduce time range.")
            srv
          case rv: RangeVector =>
            // materialize, and limit rows per RV
            val srv = SerializableRangeVector(rv, builder, recSchema, printTree(false))
            numResultSamples += srv.numRows
            // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
            if (enforceLimit && numResultSamples > limit)
              throw new BadQueryException(s"This query results in more than $limit samples. " +
                s"Try applying more filters or reduce time range.")
            srv
        }

      if (!enforceLimit) {
        finalRes = finalRes.take(limit)
      }

      finalRes.toListL
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
          qLogger.error(s"queryId: ${id} Exception during execution of query: ${printTree(false)}", ex)
          QueryError(id, ex)
        }
    }.flatten
    .onErrorRecover { case NonFatal(ex) =>
      qLogger.error(s"queryId: ${id} Exception during orchestration of query: ${printTree(false)}", ex)
      QueryError(id, ex)
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
    *
    * @param useNewline pass false if the result string needs to be in one line
    */
  final def printTree(useNewline: Boolean = true, level: Int = 0): String = {
    val transf = printRangeVectorTransformersForLevel(level)
    val nextLevel = rangeVectorTransformers.size + level
    val curNode = s"${"-"*nextLevel}E~${getClass.getSimpleName}($args) on ${dispatcher}"
    val childr = children.map(_.printTree(useNewline, nextLevel + 1))
    ((transf :+ curNode) ++ childr).mkString(if (useNewline) "\n" else " @@@ ")
  }

  final def getPlan(level: Int = 0): Seq[String] = {
    val transf = printRangeVectorTransformersForLevel(level)
    val nextLevel = rangeVectorTransformers.size + level
    val curNode = s"${"-"*nextLevel}E~${getClass.getSimpleName}($args) on ${dispatcher}"
    val childr : Seq[String]= children.flatMap(_.getPlan(nextLevel + 1))
    ((transf :+ curNode) ++ childr)
  }

  protected def printRangeVectorTransformersForLevel(level: Int = 0) = {
     rangeVectorTransformers.reverse.zipWithIndex.map { case (t, i) =>
      s"${"-" * (level + i)}T~${t.getClass.getSimpleName}(${t.args})"
    }
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
    val spanFromHelper = Kamon.currentSpan()
    val childTasks = Observable.fromIterable(children.zipWithIndex)
                               .mapAsync(Runtime.getRuntime.availableProcessors()) { case (plan, i) =>
      Kamon.withSpan(spanFromHelper) {
        plan.dispatcher.dispatch(plan).onErrorHandle { case ex: Throwable =>
          qLogger.error(s"queryId: ${id} Execution failed for sub-query ${plan.printTree()}", ex)
          QueryError(id, ex)
        }.map((_, i))
      }
    }
    compose(dataset, childTasks, queryConfig)
  }

  final protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = schemaOfCompose(dataset)

  /**
    * Schema of the RangeVectors returned by compose() method
    */
  protected def schemaOfCompose(dataset: Dataset): ResultSchema

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the sub-query results here.
    *
    * @param childResponses observable of a pair. First element of pair is the QueryResponse for
    *                       a child ExecPlan, the second element is the index of the child plan.
    *                       There is one response per child plan.
    */
  protected def compose(dataset: Dataset,
                        childResponses: Observable[(QueryResponse, Int)],
                        queryConfig: QueryConfig): Observable[RangeVector]

}
