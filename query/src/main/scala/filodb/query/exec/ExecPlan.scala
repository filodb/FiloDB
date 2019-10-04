package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.observables.ConnectableObservable

import filodb.core.DatasetRef
import filodb.core.memstore.FiloSchedulers
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.query.{RangeVector, ResultSchema, SerializableRangeVector}
import filodb.core.store.ChunkSource
import filodb.memory.format.RowReader
import filodb.query._
import filodb.query.Query.qLogger

/**
 * The observable of vectors and the schema that is returned by ExecPlan doExecute
 */
final case class ExecResult(rvs: Observable[RangeVector], schema: Task[ResultSchema])

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

  protected def allTransformers: Seq[RangeVectorTransformer] = rangeVectorTransformers

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
  def execute(source: ChunkSource, queryConfig: QueryConfig)
             (implicit sched: Scheduler, timeout: FiniteDuration): Task[QueryResponse] = {
    // NOTE: we launch the preparatory steps as a Task too.  This is important because scanPartitions,
    // Lucene index lookup, and On-Demand Paging orchestration work could suck up nontrivial time and
    // we don't want these to happen in a single thread.
    // Step 1: initiate doExecute, get schema
    lazy val step1 = Task {
      FiloSchedulers.assertThreadName(QuerySchedName)
      qLogger.debug(s"queryId: ${id} Setting up ExecPlan ${getClass.getSimpleName} with $args")
      doExecute(source, queryConfig)
    }

    // Step 2: Set up transformers and loop over all rangevectors, creating the result
    def step2(res: ExecResult) = res.schema.map { resSchema =>
      // It is possible a null schema is returned (due to no time series). In that case just return empty results
      val resultTask = if (resSchema == ResultSchema.empty) {
        qLogger.debug(s"Empty plan $this, returning empty results")
        Task.eval(QueryResult(id, resSchema, Nil))
      } else {
        val finalRes = allTransformers.foldLeft((res.rvs, resSchema)) { (acc, transf) =>
          qLogger.debug(s"queryId: ${id} Setting up Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
          (transf.apply(acc._1, queryConfig, limit, acc._2), transf.schema(acc._2))
        }
        val recSchema = SerializableRangeVector.toSchema(finalRes._2.columns, finalRes._2.brSchemas)
        val builder = SerializableRangeVector.newBuilder()
        var numResultSamples = 0 // BEWARE - do not modify concurrently!!
        finalRes._1
          .map {
            case srv: SerializableRangeVector =>
              numResultSamples += srv.numRowsInt
              // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
              if (enforceLimit && numResultSamples > limit)
                throw new BadQueryException(s"This query results in more than $limit samples. " +
                  s"Try applying more filters or reduce time range.")
              srv
            case rv: RangeVector =>
              // materialize, and limit rows per RV
              val srv = SerializableRangeVector(rv, builder, recSchema, printTree(false))
              numResultSamples += srv.numRowsInt
              // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
              if (enforceLimit && numResultSamples > limit)
                throw new BadQueryException(s"This query results in more than $limit samples. " +
                  s"Try applying more filters or reduce time range.")
              srv
          }
          .take(limit)
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
      }
      resultTask.onErrorHandle { case ex: Throwable =>
        if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
          qLogger.error(s"queryId: ${id} Exception during execution of query: ${printTree(false)}", ex)
        QueryError(id, ex)
      }
    }.flatten
    .onErrorRecover { case NonFatal(ex) =>
      if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
        qLogger.error(s"queryId: ${id} Exception during orchestration of query: ${printTree(false)}", ex)
      QueryError(id, ex)
    }

    for { res <- step1
          qResult <- step2(res) }
    yield { qResult }
  }

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node.  It will transform or produce an Observable of RangeVectors, as well as output a ResultSchema
    * that has the schema of the produced RangeVectors.
    * Note that this should not include any operations done in the transformers.
    */
  def doExecute(source: ChunkSource,
                queryConfig: QueryConfig)
               (implicit sched: Scheduler,
                timeout: FiniteDuration): ExecResult

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

  protected def rowIterAccumulator(srvsList: List[Seq[RangeVector]]): Iterator[RowReader] = {

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

  private var multicast: ConnectableObservable[(QueryResponse, Int)] = _

  private def dispatchRemotePlan(plan: ExecPlan, span: kamon.trace.Span)
                                (implicit sched: Scheduler, timeout: FiniteDuration) =
    Kamon.withSpan(span) {
      plan.dispatcher.dispatch(plan).onErrorHandle { case ex: Throwable =>
        qLogger.error(s"queryId: ${id} Execution failed for sub-query ${plan.printTree()}", ex)
        QueryError(id, ex)
      }
    }

  /**
    * Being a non-leaf node, this implementation encompasses the logic
    * of child plan execution. It then composes the sub-query results
    * using the abstract method 'compose' to arrive at the higher level
    * result.
    * The schema from all the tasks are checked; empty results are dropped and schema is determined
    * from the non-empty results.
    */
  final def doExecute(source: ChunkSource,
                      queryConfig: QueryConfig)
                     (implicit sched: Scheduler,
                      timeout: FiniteDuration): ExecResult = {
    val spanFromHelper = Kamon.currentSpan()
    // Create tasks for all results
    val childTasks = Observable.fromIterable(children)
                               .mapAsync(Runtime.getRuntime.availableProcessors()) { plan =>
                                 dispatchRemotePlan(plan, spanFromHelper)
                               }

    // The first valid schema is returned as the Task.  If all results are empty, then return
    // an empty schema.  Validate that the other schemas are the same.  Skip over empty schemas.
    var sch = ResultSchema.empty
    val processedTasks = childTasks.zipWithIndex.collect {
      case (res @ QueryResult(_, schema, _), i) if schema != ResultSchema.empty =>
        sch = reduceSchemas(sch, res, i.toInt)
        (res, i.toInt)
      case (e: QueryError, i) =>
        (e, i.toInt)
    // cache caches results so that multiple subscribers can process
    }.cache

    val outputSchema = processedTasks.collect {
      case (QueryResult(_, schema, _), _) => schema
    }.firstOptionL.map(_.getOrElse(ResultSchema.empty))

    val outputRvs = compose(processedTasks, outputSchema, queryConfig)
    ExecResult(outputRvs, outputSchema)
  }

  /**
   * Reduces the different ResultSchemas coming from each child to a single one.
   * The default one here takes the first schema response, and checks that subsequent ones match the first one.
   * Can be overridden if needed.
   * @param rs the ResultSchema from previous calls to reduceSchemas / previous child nodes.  May be empty for first.
   */
  def reduceSchemas(rs: ResultSchema, resp: QueryResult, i: Int): ResultSchema = {
    resp match {
      case QueryResult(_, schema, _) if rs == ResultSchema.empty =>
        schema     /// First schema, take as is
      case QueryResult(_, schema, _) =>
        if (rs != schema) throw new IllegalArgumentException(s"Schema mismatch: $rs != $schema")
        else rs
    }
  }

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the sub-query results here.
    *
    * @param childResponses observable of a pair. First element of pair is the QueryResponse for
    *                       a child ExecPlan, the second element is the index of the child plan.
    *                       There is one response per child plan.
    * @param firstSchema Task for the first schema coming in from the first child
    */
  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        queryConfig: QueryConfig): Observable[RangeVector]

}
