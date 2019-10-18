package filodb.query.exec

import filodb.core.DatasetRef
import filodb.core.memstore.FiloSchedulers
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.query.{RangeVector, ResultSchema, ScalarFixedDouble, ScalarVaryingDouble, ScalarVector, SerializableRangeVector, SerializedRangeVector}
import filodb.core.store.ChunkSource
import filodb.memory.format.RowReader
import filodb.query.Query.qLogger
import filodb.query._
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

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
trait ExecPlan extends QueryCommand  {
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

  final def replaceTransformers(newTransformers: Seq[RangeVectorTransformer]): Unit = {
    qLogger.debug(s"Replacing $rangeVectorTransformers with $newTransformers")
    rangeVectorTransformers.clear()
    rangeVectorTransformers ++= newTransformers
  }

  /**
    * Schema of QueryResponse returned by running execute()
    */
  final def schema(): ResultSchema = {
    val source = schemaOfDoExecute()
    rangeVectorTransformers.foldLeft(source) { (acc, transf) => transf.schema(acc) }
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
  def execute(source: ChunkSource, queryConfig: QueryConfig)
             (implicit sched: Scheduler, timeout: FiniteDuration): Task[QueryResponse] = {
    // NOTE: we launch the preparatory steps as a Task too.  This is important because scanPartitions,
    // Lucene index lookup, and On-Demand Paging orchestration work could suck up nontrivial time and
    // we don't want these to happen in a single thread.
    Task {
      FiloSchedulers.assertThreadName(QuerySchedName)
      qLogger.debug(s"queryId: ${id} Setting up ExecPlan ${getClass.getSimpleName} with $args")
      val res = doExecute(source, queryConfig)
     // val paramsRes = executeFunctionParams
      val resSchema = schemaOfDoExecute()
      val finalRes = rangeVectorTransformers.foldLeft((res, resSchema)) { (acc, transf) =>
        qLogger.debug(s"queryId: ${id} Setting up Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
        val paramRangeVector :Observable[ScalarVector] = if(transf.funcParams.isEmpty){
           Observable.empty
        } else {
          transf.funcParams.head.getResult
        }

        (transf.apply(acc._1, queryConfig, limit, acc._2, paramRangeVector), transf.schema(acc._2))
      }
      val recSchema = SerializedRangeVector.toSchema(finalRes._2.columns, finalRes._2.brSchemas)
      val builder = SerializedRangeVector.newBuilder()
      var numResultSamples = 0 // BEWARE - do not modify concurrently!!
      finalRes._1
        .map {

          case srv: SerializedRangeVector  =>
            numResultSamples += srv.numRowsInt
            // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
            if (enforceLimit && numResultSamples > limit)
              throw new BadQueryException(s"This query results in more than $limit samples. " +
                s"Try applying more filters or reduce time range.")
            srv

          case srv: SerializableRangeVector  =>
            numResultSamples += srv.numRowsInt
            // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
            if (enforceLimit && numResultSamples > limit)
              throw new BadQueryException(s"This query results in more than $limit samples. " +
                s"Try applying more filters or reduce time range.")
            srv

          case rv: RangeVector =>
            // materialize, and limit rows per RV
            val srv = SerializedRangeVector(rv, builder, recSchema, printTree(false))
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
          SerializedRangeVector.queryResultBytes.record(numBytes)
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
  }


  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  protected def doExecute(source: ChunkSource,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector]

  /**
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  protected def schemaOfDoExecute(): ResultSchema

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

sealed trait FuncArgs{
  def getResult (implicit sched: Scheduler, timeout: FiniteDuration) : Observable[ScalarVector]
}
case class ExecPlanFuncArgs(execPlan: ExecPlan) extends FuncArgs {
  override def getResult(implicit sched: Scheduler, timeout: FiniteDuration): Observable[ScalarVector] = {
    Observable.now(execPlan)
      .mapAsync(Runtime.getRuntime.availableProcessors()) { plan =>
        plan.dispatcher.dispatch(plan).onErrorHandle { case ex: Throwable =>
          qLogger.error(s"queryId: ${execPlan.id} Execution failed for sub-query ${plan.printTree()}", ex)
          QueryError(execPlan.id, ex)
        }
      }.map {
      case (QueryResult(_, _, result) )=> result.head.asInstanceOf[ScalarVaryingDouble]
      case (QueryError(_, ex)) => throw ex
    }
  }
}

case class StaticFuncArgs(scalar: Double, timeStepParams: TimeStepParams = TimeStepParams(0,0,0)) extends FuncArgs {
  override def getResult(implicit sched: Scheduler, timeout: FiniteDuration): Observable[ScalarVector] = {
    Observable.now(
     new ScalarFixedDouble(timeStepParams.start, timeStepParams.end, timeStepParams.step, scalar))
  }
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
                                queryConfig: QueryConfig)
                               (implicit sched: Scheduler,
                                timeout: FiniteDuration): Observable[RangeVector] = {
    val spanFromHelper = Kamon.currentSpan()
    val childTasks  = Observable.fromIterable(children.zipWithIndex)
                               .mapAsync(Runtime.getRuntime.availableProcessors()) { case (plan, i) =>
      Kamon.withSpan(spanFromHelper) {
        plan.dispatcher.dispatch(plan).onErrorHandle { case ex: Throwable =>
          qLogger.error(s"queryId: ${id} Execution failed for sub-query ${plan.printTree()}", ex)
          QueryError(id, ex)
        }.map((_, i))
      }
    }
    compose(childTasks, queryConfig)

  }

  final protected def schemaOfDoExecute(): ResultSchema = schemaOfCompose()

  /**
    * Schema of the RangeVectors returned by compose() method
    */
  protected def schemaOfCompose(): ResultSchema

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the sub-query results here.
    *
    * @param childResponses observable of a pair. First element of pair is the QueryResponse for
    *                       a child ExecPlan, the second element is the index of the child plan.
    *                       There is one response per child plan.
    */
  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        queryConfig: QueryConfig): Observable[RangeVector]

}
