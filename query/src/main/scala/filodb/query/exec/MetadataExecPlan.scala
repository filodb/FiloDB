package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{MemStore, PartKeyRowReader}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.PartitionSchema
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.{UTF8MapIteratorRowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger

final case class PartKeysDistConcatExec(queryContext: QueryContext,
                                        dispatcher: PlanDispatcher,
                                        children: Seq[ExecPlan]) extends NonLeafExecPlan {

  require(!children.isEmpty)

  override def enforceLimit: Boolean = false

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = ""

  /**
    * Compose the sub-query/leaf results here.
    */
  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result), _) => result
      case (QueryError(_, ex), _)         => throw ex
    }.toListL.map { resp =>
      IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), rowIterAccumulator(resp))
    }
    Observable.fromTask(taskOfResults)
  }

}

final case class LabelValuesDistConcatExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           children: Seq[ExecPlan]) extends NonLeafExecPlan {

  require(!children.isEmpty)

  override def enforceLimit: Boolean = false

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = ""

  /**
    * Compose the sub-query/leaf results here.
    */
  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result), _) => result
      case (QueryError(_, ex), _)         => throw ex
    }.toListL.map { resp =>
      var metadataResult = scala.collection.mutable.Set.empty[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
      resp.foreach { rv =>
          metadataResult ++= rv.head.rows.map { rowReader =>
            val binaryRowReader = rowReader.asInstanceOf[BinaryRecordRowReader]
            rv.head match {
              case srv: SerializedRangeVector =>
                srv.schema.toStringPairs (binaryRowReader.recordBase, binaryRowReader.recordOffset)
                   .map (pair => pair._1.utf8 -> pair._2.utf8).toMap
              case _ => throw new UnsupportedOperationException("Metadata query currently needs SRV results")
            }
          }
      }
      //distinct -> result may have duplicates in case of labelValues
      IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new UTF8MapIteratorRowReader(metadataResult.toIterator))
    }
    Observable.fromTask(taskOfResults)
  }

}

final case class PartKeysExec(queryContext: QueryContext,
                              dispatcher: PlanDispatcher,
                              dataset: DatasetRef,
                              shard: Int,
                              partSchema: PartitionSchema,
                              filters: Seq[ColumnFilter],
                              start: Long,
                              end: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                queryConfig: QueryConfig)
               (implicit sched: Scheduler): ExecResult = {
    val rvs = source match {
      case memStore: MemStore =>
        val response = memStore.partKeysWithFilters(dataset, shard, filters, end, start, queryContext.sampleLimit)
        Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), new PartKeyRowReader(response)))
      case other =>
        Observable.empty
    }
    Kamon.currentSpan().mark("creating-resultschema")
    val sch = new ResultSchema(Seq(ColumnInfo("TimeSeries", ColumnType.BinaryRecordColumn)), 1,
                               Map(0 -> partSchema.binSchema))
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, limit=${queryContext.sampleLimit}"
}

final case class  LabelValuesExec(queryContext: QueryContext,
                                  dispatcher: PlanDispatcher,
                                  dataset: DatasetRef,
                                  shard: Int,
                                  filters: Seq[ColumnFilter],
                                  columns: Seq[String],
                                  lookBackInMillis: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                queryConfig: QueryConfig)
               (implicit sched: Scheduler): ExecResult = {
    val parentSpan = Kamon.currentSpan()
    val rvs = if (source.isInstanceOf[MemStore]) {
      val memStore = source.asInstanceOf[MemStore]
      val curr = System.currentTimeMillis()
      val end = curr - curr % 1000 // round to the floor second
      val start = end - lookBackInMillis
      val response = filters.isEmpty match {
        // retrieves label values for a single label - no column filter
        case true if (columns.size == 1) => memStore.labelValues(dataset, shard, columns.head, queryContext.sampleLimit)
          .map(termInfo => Map(columns.head.utf8 -> termInfo.term))
          .toIterator
        case true => throw new BadQueryException("either label name is missing " +
          "or there are multiple label names without filter")
        case false => memStore.labelValuesWithFilters(dataset, shard, filters, columns, end, start,
          queryContext.sampleLimit)
      }
      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new UTF8MapIteratorRowReader(response)))
    } else {
      Observable.empty
    }
    parentSpan.mark("creating-resultschema")
    val sch = new ResultSchema(Seq(ColumnInfo("Labels", ColumnType.MapColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, col=$columns, limit=${queryContext.sampleLimit}, " +
    s"lookBackInMillis=$lookBackInMillis"
}
