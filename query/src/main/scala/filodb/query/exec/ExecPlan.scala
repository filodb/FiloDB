package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, Pipe}

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.{FiloSchedulers, SchemaMismatch}
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.query._
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
    * Query Processing parameters
    */
  def queryContext: QueryContext

  /**
    * Throw error if the size of the resultset is greater than Limit
    * Take first n (limit) elements if the flag is false. Applicable for Metadata Queries
    * It is not in QueryContext since for some queries it should be false
    */
  def enforceSampleLimit: Boolean = true

  /**
   * Default max size of the record containers created for the ExecPlan. Specific ExecPlans may choose to override and
   * use different values
   *
   * @return
   */
  def maxRecordContainerSize: Int = SerializedRangeVector.MaxContainerSize

  /**
    * Child execution plans representing sub-queries
    */
  def children: Seq[ExecPlan]

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
   * This first invokes the doExecute abstract method, then applies
   * the RangeVectorMappers associated with this plan node.
   *
   * The response is a stream of StreamQueryResponse Observable which can
   * be streamed out to the wire, or processed as optimally appropriate.
   * The advantage of using this over `execute` is that this method uses
   * much less memory than when converted to a fat response object.
   *
   * Typically the caller creates the QuerySession parameter object. Remember
   * that the creator is also responsible for closing it with
   * `returnObservable.guarantee(Task.eval(querySession.close()))`
   *
   */
  // scalastyle:off method.length
  def executeStreaming(source: ChunkSource,
                       querySession: QuerySession)
                       (implicit sched: Scheduler): Observable[StreamQueryResponse] = {

    val startExecute = querySession.qContext.submitTime

    val span = Kamon.currentSpan()
    // NOTE: we launch the preparatory steps as a Task too.  This is important because scanPartitions,
    // Lucene index lookup, and On-Demand Paging orchestration work could suck up nontrivial time and
    // we don't want these to happen in a single thread.

    // Step 1: initiate doExecute pipeline: make result schema and set up the async monix pipeline to create RVs
    lazy val step1: Task[ExecResult] = Task.evalAsync {
      // avoid any work when plan has waited in executor queue for long
      queryContext.checkQueryTimeout(s"step1-${this.getClass.getSimpleName}")
      span.mark(s"execute-step1-start-${getClass.getSimpleName}")
      FiloSchedulers.assertThreadName(QuerySchedName)
      // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
      // across threads. Note that task/observable will not run on the thread where span is present since
      // kamon uses thread-locals.
      // Dont finish span since this code didnt create it
      Kamon.runWithSpan(span, false) {
        val doEx = doExecute(source, querySession)
        Kamon.histogram("query-execute-time-elapsed-step1-done",
          MeasurementUnit.time.milliseconds)
          .withTag("plan", getClass.getSimpleName)
          .record(Math.max(0, System.currentTimeMillis - startExecute))
        span.mark(s"execute-step1-end-${getClass.getSimpleName}")
        doEx
      }
    }

    // Step 2: Append transformer execution to step1 result, materialize the final result
    def step2(res: ExecResult): Observable[StreamQueryResponse] = {
      val task = res.schema.map { resSchema =>
        // avoid any work when plan has waited in executor queue for long
        queryContext.checkQueryTimeout(s"step2-${this.getClass.getSimpleName}")
        Kamon.histogram("query-execute-time-elapsed-step2-start", MeasurementUnit.time.milliseconds)
          .withTag("plan", getClass.getSimpleName)
          .record(Math.max(0, System.currentTimeMillis - startExecute))
        FiloSchedulers.assertThreadName(QuerySchedName)
        val finalRes = allTransformers.foldLeft((res.rvs, resSchema)) { (acc, transf) =>
          val paramRangeVector: Seq[Observable[ScalarRangeVector]] =
            transf.funcParams.map(_.getResult(querySession, source))
          val resultSchema : ResultSchema = acc._2
          if (resultSchema == ResultSchema.empty && (!transf.canHandleEmptySchemas)) {
            // It is possible a null schema is returned (due to no time series). In that case just skip the
            // transformers that cannot handle empty results
            (acc._1, resultSchema)
          } else {
            val rangeVector : Observable[RangeVector] = transf.apply(
              acc._1, querySession, queryContext.plannerParams.sampleLimit, acc._2, paramRangeVector
            )
            val schema = transf.schema(resultSchema)
            (rangeVector, schema)
          }
        }
        if (finalRes._2 == ResultSchema.empty) {
          Observable.fromIterable(Seq(StreamQueryResultHeader(queryContext.queryId, finalRes._2),
            StreamQueryResultFooter(queryContext.queryId, querySession.queryStats,
              querySession.resultCouldBePartial, querySession.partialResultsReason)))
        } else {
          val recSchema = SerializedRangeVector.toSchema(finalRes._2.columns, finalRes._2.brSchemas)
          Kamon
            .histogram(
              "query-execute-time-elapsed-step2-transformer-pipeline-setup",
              MeasurementUnit.time.milliseconds
            )
            .withTag("plan", getClass.getSimpleName)
            .record(Math.max(0, System.currentTimeMillis - startExecute))
          makeResult(finalRes._1, recSchema, finalRes._2)
        }
      }
      Observable.fromTask(task).flatten
    }

    def makeResult(rv: Observable[RangeVector], recordSchema: RecordSchema,
                   resultSchema: ResultSchema): Observable[StreamQueryResponse] = {
      @volatile var numResultSamples = 0 // BEWARE - do not modify concurrently!!
      @volatile var resultSize = 0L
      val queryResults = rv.doOnStart(_ => Task.eval(span.mark("before-first-materialized-result-rv")))
        .map {
          case srvable: SerializableRangeVector => srvable
          case rv: RangeVector =>
            // materialize, and limit rows per RV
            val execPlanString = queryWithPlanName(queryContext)
            val builder = SerializedRangeVector.newBuilder()
            val srv = SerializedRangeVector(rv, builder, recordSchema, execPlanString)
            if (rv.outputRange.isEmpty)
              qLogger.debug(s"Empty rangevector found. Rv class is:  ${rv.getClass.getSimpleName}, " +
                s"execPlan is: $execPlanString, execPlan children ${this.children}")
            srv
        }
        .map { srv =>
          numResultSamples += srv.numRowsSerialized
          // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
          if (enforceSampleLimit && numResultSamples > queryContext.plannerParams.sampleLimit)
            throw new BadQueryException(s"This query results in more than ${queryContext.plannerParams.
              sampleLimit} samples. Try applying more filters or reduce time range.")

          resultSize += srv.estimateSerializedRowBytes + srv.key.keySize
          if (resultSize > queryContext.plannerParams.resultByteLimit) {
            val size_mib = queryContext.plannerParams.resultByteLimit / math.pow(1024, 2)
            val msg = s"Reached maximum result size (final or intermediate) " +
              s"for data serialized out of a host or shard " +
              s"(${math.round(size_mib)} MiB)."
            qLogger.warn(s"$msg QueryContext: $queryContext")
            if (querySession.queryConfig.enforceResultByteLimit) {
              throw new BadQueryException(
                s"$msg Try to apply more filters, reduce the time range, and/or increase the step size.")
            }
          }
          srv
        }
        .filter(_.numRowsSerialized > 0)
        .map(StreamQueryResult(queryContext.queryId, _))
        .guarantee(Task.eval {
          Kamon.histogram("query-execute-time-elapsed-step2-result-materialized",
            MeasurementUnit.time.milliseconds)
            .withTag("plan", getClass.getSimpleName)
            .record(Math.max(0, System.currentTimeMillis - startExecute))
          SerializedRangeVector.queryResultBytes.record(resultSize)
          querySession.queryStats.getResultBytesCounter(Nil).addAndGet(resultSize)
          span.mark("after-last-materialized-result-rv")
          span.mark(s"resultBytes=$resultSize")
          span.mark(s"resultSamples=$numResultSamples")
          span.mark(s"execute-step2-end-${this.getClass.getSimpleName}")
        })

      Observable.now(StreamQueryResultHeader(queryContext.queryId, resultSchema)) ++
        queryResults ++
        Observable.now(StreamQueryResultFooter(queryContext.queryId, querySession.queryStats,
          querySession.resultCouldBePartial, querySession.partialResultsReason))
    }

    val qResult = step1.map(er => step2(er))
    val ret = Observable.fromTask(qResult).flatten
    qLogger.debug(s"Constructed monix query execution pipeline for $this")
    ret
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
  * Typically the caller creates the QuerySession parameter object. Remember
  * that the creator is also responsible for closing it with
  * `returnTask.guarantee(Task.eval(querySession.close()))`
  *
  */
  // scalastyle:off method.length
  def execute(source: ChunkSource,
              querySession: QuerySession)
             (implicit sched: Scheduler): Task[QueryResponse] = {

    val startExecute = querySession.qContext.submitTime

    val span = Kamon.currentSpan()
    // NOTE: we launch the preparatory steps as a Task too.  This is important because scanPartitions,
    // Lucene index lookup, and On-Demand Paging orchestration work could suck up nontrivial time and
    // we don't want these to happen in a single thread.

    // Step 1: initiate doExecute: make result schema and set up the async monix pipeline to create RVs
    lazy val step1: Task[ExecResult] = Task.evalAsync {
      // avoid any work when plan has waited in executor queue for long
      queryContext.checkQueryTimeout(s"step1-${this.getClass.getSimpleName}")
      span.mark(s"execute-step1-start-${getClass.getSimpleName}")
      FiloSchedulers.assertThreadName(QuerySchedName)
      // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
      // across threads. Note that task/observable will not run on the thread where span is present since
      // kamon uses thread-locals.
      // Dont finish span since this code didnt create it
      Kamon.runWithSpan(span, false) {
        val doEx = doExecute(source, querySession)
        Kamon.histogram("query-execute-time-elapsed-step1-done",
          MeasurementUnit.time.milliseconds)
          .withTag("plan", getClass.getSimpleName)
          .record(Math.max(0, System.currentTimeMillis - startExecute))
        span.mark(s"execute-step1-end-${getClass.getSimpleName}")
        doEx
      }
    }

    // Step 2: Append transformer execution to step1 result, materialize the final result
    def step2(res: ExecResult): Task[QueryResponse] = res.schema.flatMap { resSchema =>
      // avoid any work when plan has waited in executor queue for long
      queryContext.checkQueryTimeout(s"step2-${this.getClass.getSimpleName}")
      Kamon.histogram("query-execute-time-elapsed-step2-start", MeasurementUnit.time.milliseconds)
        .withTag("plan", getClass.getSimpleName)
        .record(Math.max(0, System.currentTimeMillis - startExecute))
      span.mark(s"execute-step2-start-${getClass.getSimpleName}")
      FiloSchedulers.assertThreadName(QuerySchedName)
      val resultTask = {
        val finalRes = allTransformers.foldLeft((res.rvs, resSchema)) { (acc, transf) =>
          val paramRangeVector: Seq[Observable[ScalarRangeVector]] =
                transf.funcParams.map(_.getResult(querySession, source))
          val resultSchema : ResultSchema = acc._2
          if (resultSchema == ResultSchema.empty && (!transf.canHandleEmptySchemas)) {
            // It is possible a null schema is returned (due to no time series). In that case just skip the
            // transformers that cannot handle empty results
            (acc._1, resultSchema)
          } else {
            val rangeVector : Observable[RangeVector] = transf.apply(
              acc._1, querySession, queryContext.plannerParams.sampleLimit, acc._2, paramRangeVector
            )
            val schema = transf.schema(resultSchema)
            (rangeVector, schema)
          }
        }
        if (finalRes._2 == ResultSchema.empty) {
          span.mark("empty-plan")
          span.mark(s"execute-step2-end-${getClass.getSimpleName}")
          Task.eval( {
            qLogger.debug(s"Finished query execution pipeline with empty results for $this")
            QueryResult(queryContext.queryId, resSchema, Nil, querySession.queryStats,
              querySession.resultCouldBePartial, querySession.partialResultsReason
            )
          })
        } else {
          val recSchema = SerializedRangeVector.toSchema(finalRes._2.columns, finalRes._2.brSchemas)
          Kamon
            .histogram(
            "query-execute-time-elapsed-step2-transformer-pipeline-setup",
              MeasurementUnit.time.milliseconds
            )
            .withTag("plan", getClass.getSimpleName)
            .record(Math.max(0, System.currentTimeMillis - startExecute))
          makeResult(finalRes._1, recSchema, finalRes._2)
        }
      }
      resultTask.onErrorHandle { case ex: Throwable =>
        QueryError(queryContext.queryId, querySession.queryStats, ex)
      }
    }

    def makeResult(
      rv : Observable[RangeVector], recordSchema: RecordSchema, resultSchema: ResultSchema
    ): Task[QueryResult] = {
        @volatile var numResultSamples = 0 // BEWARE - do not modify concurrently!!
        @volatile var resultSize = 0L
        val builder = SerializedRangeVector.newBuilder(maxRecordContainerSize)
        rv.doOnStart(_ => Task.eval(span.mark("before-first-materialized-result-rv")))
          .map {
            case srvable: SerializableRangeVector => srvable
            case rv: RangeVector =>
              // materialize, and limit rows per RV
              val execPlanString = queryWithPlanName(queryContext)
              val srv = SerializedRangeVector(rv, builder, recordSchema, execPlanString)
              if (rv.outputRange.isEmpty)
                qLogger.debug(s"Empty rangevector found. Rv class is:  ${rv.getClass.getSimpleName}, " +
                  s"execPlan is: $execPlanString, execPlan children ${this.children}")
              srv
          }
          .map { srv =>
              numResultSamples += srv.numRowsSerialized
              // fail the query instead of limiting range vectors and returning incomplete/inaccurate results
              if (enforceSampleLimit && numResultSamples > queryContext.plannerParams.sampleLimit)
                throw new BadQueryException(s"This query results in more than ${queryContext.plannerParams.
                  sampleLimit} samples. Try applying more filters or reduce time range.")

              resultSize += srv.estimateSerializedRowBytes + srv.key.keySize
              if (resultSize > queryContext.plannerParams.resultByteLimit) {
                val size_mib = queryContext.plannerParams.resultByteLimit / math.pow(1024, 2)
                val msg = s"Reached maximum result size (final or intermediate) " +
                          s"for data serialized out of a host or shard " +
                          s"(${math.round(size_mib)} MiB)."
                qLogger.warn(s"$msg QueryContext: $queryContext")
                if (querySession.queryConfig.enforceResultByteLimit) {
                  throw new BadQueryException(
                    s"$msg Try to apply more filters, reduce the time range, and/or increase the step size.")
                }
              }

              srv
          }
          .filter(_.numRowsSerialized > 0)
          .guarantee(Task.eval(span.mark("after-last-materialized-result-rv")))
          .toListL
          .map { r =>
            Kamon.histogram("query-execute-time-elapsed-step2-result-materialized",
                  MeasurementUnit.time.milliseconds)
              .withTag("plan", getClass.getSimpleName)
              .record(Math.max(0, System.currentTimeMillis - startExecute))
            SerializedRangeVector.queryResultBytes.record(resultSize)
            querySession.queryStats.getResultBytesCounter(Nil).addAndGet(resultSize)
            span.mark(s"resultBytes=$resultSize")
            span.mark(s"resultSamples=$numResultSamples")
            span.mark(s"numSrv=${r.size}")
            span.mark(s"execute-step2-end-${this.getClass.getSimpleName}")
            qLogger.debug(s"Finished query execution pipeline with ${r.size} RVs for $this")
            QueryResult(queryContext.queryId, resultSchema, r, querySession.queryStats,
              querySession.resultCouldBePartial, querySession.partialResultsReason)
          }
    }

    val qresp = for { res <- step1
                    qResult <- step2(res) }
              yield { qResult }
    val ret = qresp.onErrorRecover { case NonFatal(ex) =>
      QueryError(queryContext.queryId, querySession.queryStats, ex)
    }
    qLogger.debug(s"Constructed monix query execution pipeline for $this")
    ret
  }

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node.  It will transform or produce an Observable of RangeVectors, as well as output a ResultSchema
    * that has the schema of the produced RangeVectors.
    * Note that this should not include any operations done in the transformers.
    */
  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult

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
  final def printTree(useNewline: Boolean = true,
                      level: Int = 0): String = {
    val transf = printRangeVectorTransformersForLevel(level)
    val nextLevel = rangeVectorTransformers.size + level
    val curNode = curNodeText(nextLevel)
    val childr = children.map(_.printTree(useNewline, nextLevel + 1))
    ((transf :+ curNode) ++ childr).mkString(if (useNewline) "\n" else " @@@ ")
  }

  protected def queryWithPlanName(queryContext: QueryContext): String = {
    s"${this.getClass.getSimpleName}-${queryContext.origQueryParams}"
  }

  def curNodeText(level: Int): String =
    s"${"-"*level}E~${getClass.getSimpleName}($args) on ${dispatcher}"

  final def getPlan(level: Int = 0): Seq[String] = {
    val transf = printRangeVectorTransformersForLevel(level).flatMap(x => x.split("\n"))
    val nextLevel = rangeVectorTransformers.size + level
    val curNode = s"${"-"*nextLevel}E~${getClass.getSimpleName}($args) on ${dispatcher}"
    val childr : Seq[String]= children.flatMap(_.getPlan(nextLevel + 1))
    (transf :+ curNode) ++ childr
  }

  protected def printRangeVectorTransformersForLevel(level: Int = 0) = {
     rangeVectorTransformers.reverse.zipWithIndex.map { case (t, i) =>
      s"${"-" * (level + i)}T~${t.getClass.getSimpleName}(${t.args})" +
       printFunctionArgument(t, level + i + 1).mkString("\n")
    }
  }

  protected def printFunctionArgument(rvt: RangeVectorTransformer, level: Int) = {
    if (rvt.funcParams.isEmpty) {
      Seq("")
    } else {
      rvt.funcParams.zipWithIndex.map { case (f, i) =>
        val prefix = s"\n${"-" * (level)}FA${i + 1}~"
        f match {
          case e: ExecPlanFuncArgs => prefix + "\n" + e.execPlan.printTree(true, level)
          case _                   => prefix + f.toString
        }
      }
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
  final def submitTime: Long = queryContext.submitTime
}

/**
  * Function Parameter for RangeVectorTransformer
  * getResult will get the ScalarRangeVector for the FuncArg
  */
sealed trait FuncArgs {
  def getResult(querySession: QuerySession,
                source: ChunkSource)(implicit sched: Scheduler) : Observable[ScalarRangeVector]
}

/**
  * FuncArgs for ExecPlan
  */
final case class ExecPlanFuncArgs(execPlan: ExecPlan, timeStepParams: RangeParams) extends FuncArgs {

  override def getResult(querySession: QuerySession,
                         source: ChunkSource)(implicit sched: Scheduler): Observable[ScalarRangeVector] = {
    Observable.fromTask(
      execPlan.dispatcher.dispatch(ExecPlanWithClientParams(execPlan,
        ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis - 1000)), source)
          .onErrorHandle { ex: Throwable =>
            QueryError(execPlan.queryContext.queryId, querySession.queryStats, ex)
          }.map {
            case QueryResult(_, _, result, qStats, isPartialResult, partialResultReason)  =>
                          querySession.queryStats.add(qStats)
                          // Result is empty because of NaN so create ScalarFixedDouble with NaN
                          if (isPartialResult) {
                            querySession.resultCouldBePartial = true
                            querySession.partialResultsReason = partialResultReason
                          }

                          if (result.isEmpty) {
                              ScalarFixedDouble(timeStepParams, Double.NaN)
                            } else {
                              result.head match {
                                case f: ScalarFixedDouble   => f
                                case s: ScalarVaryingDouble => s
                              }
                            }
            case QueryError(_, qStats, ex)          =>
                          querySession.queryStats.add(qStats)
                          throw ex
          }
    )
  }

  override def toString: String = execPlan.printTree() + "\n"
}

/**
  * FuncArgs for scalar parameter
  */
final case class StaticFuncArgs(scalar: Double, timeStepParams: RangeParams) extends FuncArgs {
  override def getResult(querySession: QuerySession,
                         source: ChunkSource)(implicit sched: Scheduler): Observable[ScalarRangeVector] = {
    Observable.now(ScalarFixedDouble(timeStepParams, scalar))
  }
}

/**
  * FuncArgs for date and time functions
  */
final case class TimeFuncArgs(timeStepParams: RangeParams) extends FuncArgs {
  override def getResult(querySession: QuerySession,
                         source: ChunkSource)(implicit sched: Scheduler): Observable[ScalarRangeVector] = {
    Observable.now(TimeScalar(timeStepParams))
  }
}

abstract class NonLeafExecPlan extends ExecPlan {

  /**
    * For now we do not support cross-dataset queries
    */
  final def dataset: DatasetRef = children.head.dataset

  final def submitTime: Long = children.head.queryContext.submitTime

  // flag to override child task execution behavior. If it is false, child tasks get executed sequentially.
  // Use-cases include splitting longer range query into multiple smaller range queries.
  def parallelChildTasks: Boolean = true

  private def dispatchRemotePlan(plan: ExecPlan, qSession: QuerySession, span: kamon.trace.Span, source: ChunkSource)
                                (implicit sched: Scheduler) = {
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(span, false) {
      plan.dispatcher.dispatch(ExecPlanWithClientParams(plan,
        ClientParams(plan.queryContext.plannerParams.queryTimeoutMillis - 1000)), source)
          .onErrorHandle { ex: Throwable =>
            QueryError(queryContext.queryId, qSession.queryStats, ex)
          }
    }
  }

  private def dispatchStreamingRemotePlan(plan: ExecPlan, qSession: QuerySession,
                                          span: kamon.trace.Span, source: ChunkSource)
                                          (implicit sched: Scheduler) = {
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(span, false) {
      plan.dispatcher.dispatchStreaming(ExecPlanWithClientParams(plan,
        ClientParams(plan.queryContext.plannerParams.queryTimeoutMillis - 1000)), source)
          .onErrorHandle { ex: Throwable =>
            StreamQueryError(queryContext.queryId, qSession.queryStats, ex)
          }
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
                      querySession: QuerySession)
                     (implicit sched: Scheduler): ExecResult = {
    val span = Kamon.currentSpan()

    span.mark(s"execute-step1-child-result-composition-start-${getClass.getSimpleName}")
    // whether child tasks need to be executed sequentially.
    // parallelism 1 means, only one worker thread to process underlying tasks.
    val parallelism: Int = if (parallelChildTasks)
                              children.length
                           else
                              1

    if (querySession.streamingDispatch) {
      // Create tasks for all results.
      // NOTE: It's really important to preserve the "index" of the child task, as joins depend on it
      var sch = ResultSchema.empty
      val childResults = Observable.fromIterable(children.zipWithIndex)
        .map { case (plan, i) =>
          val results = dispatchStreamingRemotePlan(plan, querySession, span, source)
          // find schema mismatch errors and re-throw errors
          val respWithoutErrors = results.map {
            case header @ StreamQueryResultHeader(_, rs) =>
              if (rs != ResultSchema.empty) sch = reduceSchemas(sch, rs) // error on any schema mismatch
              header
            case res @ StreamQueryResult(_, _) =>
              res
            case footer @ StreamQueryResultFooter(_, qStats, isPartialResult, partialResultReason) =>
              if (isPartialResult) {
                querySession.resultCouldBePartial = true
                querySession.partialResultsReason = partialResultReason
              }
              querySession.queryStats.add(qStats)
              footer
            case StreamQueryError(_, queryStats, t) =>
              querySession.queryStats.add(queryStats)
              throw t
          }
          (respWithoutErrors, i)
        }.pipeThrough(Pipe.publish[(Observable[StreamQueryResponse], Int)])
             // pipeThrough helps with multiple subscribers

      val schemas = childResults.flatMap { obs =>
        obs._1.find(_.isInstanceOf[StreamQueryResultHeader])
          .map(_.asInstanceOf[StreamQueryResultHeader].resultSchema)
          .map(rs => (rs, obs._2))
      }.pipeThrough(Pipe.publish[(ResultSchema, Int)]) // pipeThrough helps with multiple subscribers

      val rvs = childResults.map { obs =>
        val rvs = obs._1.collect { case s: StreamQueryResult => s.result }
        (rvs, obs._2)
      }
      val outputSchema = deriveOutputSchema(schemas)

      val results = composeStreaming(rvs, schemas, querySession)
      ExecResult(results, outputSchema)
    } else {
      // Create tasks for all results.
      // NOTE: It's really important to preserve the "index" of the child task, as joins depend on it
      val childTasks = Observable.fromIterable(children.zipWithIndex)
        .mapParallelUnordered(parallelism) { case (plan, i) =>
          val task = dispatchRemotePlan(plan, querySession, span, source).map((_, i))
          span.mark(s"child-plan-$i-dispatched-${plan.getClass.getSimpleName}")
          task
        }

      // The first valid schema is returned as the Task.  If all results are empty, then return
      // an empty schema.  Validate that the other schemas are the same.  Skip over empty schemas.
      var sch = ResultSchema.empty
      val processedTasks = childTasks
        .doOnStart(_ => Task.eval(span.mark("first-child-result-received")))
        .guarantee(Task.eval(span.mark("last-child-result-received")))
        .map {
          case (res@QueryResult(_, _, _, qStats, isPartialResult, partialResultReason), i) =>
            qLogger.debug(s"Child query result received for $this")
            if (isPartialResult) {
              querySession.resultCouldBePartial = true
              querySession.partialResultsReason = partialResultReason
            }
            querySession.queryStats.add(qStats)
          if (res.resultSchema != ResultSchema.empty) sch = reduceSchemas(sch, res.resultSchema)
            (res, i)
          case (e: QueryError, _) =>
            querySession.queryStats.add(e.queryStats)
            throw e.t
        }
        .filter(_._1.resultSchema != ResultSchema.empty)
        .cache
        //.pipeThrough(Pipe.publish[(QueryResult, Int)]) // pipeThrough helps with multiple subscribers

      val outputSchema = processedTasks.collect { // collect schema of first result that is nonEmpty
        case (QueryResult(_, schema, _, _, _, _), _) if schema.columns.nonEmpty => schema
      }.firstOptionL.map(_.getOrElse(ResultSchema.empty))
      // Dont finish span since this code didnt create it
      Kamon.runWithSpan(span, false) {
        val outputRvs = compose(processedTasks, outputSchema, querySession)
          .guaranteeCase { _ =>
            Task.eval(span.mark(s"execute-step1-child-result-composition-end-${this.getClass.getSimpleName}"))
          }
        ExecResult(outputRvs, outputSchema)
      }
    }
  }

  /**
   * Reduces the different ResultSchemas coming from each child to a single one.
   * The default one here takes the first schema response, and checks that subsequent ones match the first one.
   * Can be overridden if needed.
   * @param rs the ResultSchema from previous calls to reduceSchemas / previous child nodes.  May be empty for first.
   */
  def reduceSchemas(r1: ResultSchema, r2: ResultSchema): ResultSchema = {
    if (r1.isEmpty) r2
    else if (r2.isEmpty) r1
    else if (!r1.hasSameColumnsAs(r2))  {
      throw SchemaMismatch(r1.toString, r2.toString)
    } else {
      val fixedVecLen = if (r1.fixedVectorLen.isEmpty && r2.fixedVectorLen.isEmpty) None
      else Some(r1.fixedVectorLen.getOrElse(0) + r2.fixedVectorLen.getOrElse(0))
      r1.copy(fixedVectorLen = fixedVecLen)
    }
  }

  def deriveOutputSchema(childSchemas: Observable[(ResultSchema, Int)]): Task[ResultSchema] = {
    childSchemas.firstOptionL.map(_.map(_._1).getOrElse(ResultSchema.empty))
  }

  /**
    * Sub-class non-leaf nodes should provide their own implementation of how
    * to compose the child-query results here.
    *
    * @param childResponses observable of a pair. First element of pair is the QueryResponse for
    *                       a child ExecPlan, the second element is the index of the child plan.
    *                       There is one response per child plan.
    * @param firstSchema Task for the first schema coming in from the first child
    */
  protected def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector]

  /**
   * Sub-class non-leaf nodes should provide their own implementation of how
   * to compose the child-query results here
   *
   * @param childResponses stream of Observable-Int 2-tuple, one for each child plan
   * @param schemas stream of ResultSchema-Int 2-tuple, one for each child plan
   * @param querySession the query session for this plan execution
   * @return Stream of result range vectors
   */
  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector]

}

// deadline is set to QueryContext.plannerParams.queryTimeoutMillis for the top level call
case class ClientParams(deadline: Long)
case class ExecPlanWithClientParams(execPlan: ExecPlan, clientParams: ClientParams)
