package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}
import org.apache.datasketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.datasketches.memory.Memory
import scala.collection.mutable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.{BinaryRecordRowReader, MapItemConsumer}
import filodb.core.memstore.{MemStore, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.store.ChunkSource
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{RowReader, SeqRowReader, StringArrayRowReader, UnsafeUtils,
                             UTF8MapIteratorRowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger

trait MetadataDistConcatExec extends NonLeafExecPlan {

  require(children.nonEmpty)

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
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result, _, _, _), _) => result
      case (QueryError(_, _, ex), _)         => throw ex
    }.toListL.map { resp =>
      val metadataResult = scala.collection.mutable.Set.empty[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
      resp.foreach { rv =>
        metadataResult ++= rv.head.rows().map { rowReader =>
          val binaryRowReader = rowReader.asInstanceOf[BinaryRecordRowReader]
          rv.head match {
            case srv: SerializedRangeVector =>
              srv.schema.toStringPairs (binaryRowReader.recordBase, binaryRowReader.recordOffset)
                .map (pair => pair._1.utf8 -> pair._2.utf8).toMap
            case _ => throw new UnsupportedOperationException("Metadata query currently needs SRV results")
          }
        }
      }
      IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        UTF8MapIteratorRowReader(metadataResult.toIterator), None)
    }
    Observable.fromTask(taskOfResults)
  }
}

final case class PartKeysDistConcatExec(queryContext: QueryContext,
                                        dispatcher: PlanDispatcher,
                                        children: Seq[ExecPlan]) extends MetadataDistConcatExec

final case class MetricCardTopkMergeExec(queryContext: QueryContext,
                                         dispatcher: PlanDispatcher,
                                         children: Seq[ExecPlan],
                                         k: Int) extends NonLeafExecPlan {
  override protected def args: String = ""

  override protected def forceSchema: Option[ResultSchema] = Option(ResultSchema(
    Seq(ColumnInfo("Metric", ColumnType.StringColumn),
        ColumnInfo("Cardinality", ColumnType.LongColumn)), 1))

  private def sketchFold(acc: ItemsSketch[ZeroCopyUTF8String], rv: RangeVector):
            ItemsSketch[ZeroCopyUTF8String] = {
    rv.rows().foreach{ r =>
      val sketchSer = r.getString(0).getBytes
      val sketch = ItemsSketch.getInstance(Memory.wrap(sketchSer), new ZeroCopyUtf8SerDe)
      acc.merge(sketch)
    }
    acc
  }

  private def pqueueFold(acc: mutable.PriorityQueue[(ZeroCopyUTF8String, Long)],
                 tup: (ZeroCopyUTF8String, Long)): mutable.PriorityQueue[(ZeroCopyUTF8String, Long)] = {
    if (acc.size < k) {
      acc.enqueue(tup)
    } else if (acc.head._2 < tup._2) {
      // min count in the heap is less than the current count
      acc.dequeue()
      acc.enqueue(tup)
    }
    acc
  }

  // TODO(a_theimer): toss this in a companion object?
  private def ordering : Ordering[(ZeroCopyUTF8String, Long)] = Ordering.by(pair => -pair._2)

  override protected def compose(childResponses: Observable[(QueryResponse, Int)],
                                 firstSchema: Task[ResultSchema],
                                 querySession: QuerySession): Observable[RangeVector] = {
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result, _, _, _), _) => Observable.fromIterable(result)
      case (QueryError(_, _, ex), _)         => throw ex
    }.flatten
      // merge all of the sketches
      .foldLeftF(new ItemsSketch[ZeroCopyUTF8String](MetricCardTopkExec.MAX_ITEMSKETCH_MAP_SIZE))(sketchFold)
      // collect the most frequent rows from the union
      .flatMap(sketch => Observable.fromIterable(sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)))
      // prepare each row for use in a priority queue  // TODO(a_theimer): just use the rows themselves
      .map(row => (row.getItem, row.getEstimate))
      // build a pqueue of maximum size k
      .foldLeftL(mutable.PriorityQueue[(ZeroCopyUTF8String, Long)]()(ordering))(pqueueFold)
      // store each pqueue element into an iterable container
      .map{ topkHeap =>
        // TODO(a_theimer): more idiomatic functional-programming way to do this?
        val arrSize = math.min(k, topkHeap.size)
        val rows = new mutable.ArraySeq[RowReader](arrSize)
        for (i <- 0 until arrSize) {
          val heapMin = topkHeap.dequeue()
          rows(i) = SeqRowReader(Seq(heapMin._1, heapMin._2))
        }
        IteratorBackedRangeVector(CustomRangeVectorKey(Map.empty),
          new NoCloseCursor(rows.reverseIterator),
          None)
      }
    Observable.fromTask(taskOfResults)
  }
}

final case class LabelValuesDistConcatExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           children: Seq[ExecPlan]) extends MetadataDistConcatExec

final class LabelCardinalityPresenter(val funcParams: Seq[FuncArgs]  = Nil) extends RangeVectorTransformer {

  override def apply(source: Observable[RangeVector],
                     querySession: QuerySession,
                     limit: Int,
                     sourceSchema: ResultSchema,
                     paramsResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    source.map(rv => {
          val x = rv.rows().next()
          // TODO: We expect only one column to be a map, pattern matching does not work, is there better way?
          val sketchMap = x.getAny(columnNo = 0).asInstanceOf[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
          val sketchMapIterator = (sketchMap.mapValues {
            sketch => ZeroCopyUTF8String(Math.round(CpcSketch.heapify(sketch.bytes).getEstimate).toInt.toString)}
            :: Nil).toIterator
          IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
            UTF8MapIteratorRowReader(sketchMapIterator), None)
      })
  }

  override protected[exec] def args: String = s"LabelCardinalityPresenter"
}

final case class LabelNamesDistConcatExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           children: Seq[ExecPlan]) extends DistConcatExec {
  /**
   * Pick first non empty result from child.
   */
  override protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    childResponses.map {
      case (QueryResult(_, _, result, _, _, _), _) => result
      case (QueryError(_, _, ex), _)         => throw ex
    }.filter(s => s.head.numRows.getOrElse(1) > 0).headF.map(_.head)
  }
}

trait LabelCardinalityExecPlan {
  /**
   * Parameter deciding the sketch size to be used for approximating cardinality
   */
  val logK = 12
}
final case class LabelCardinalityReduceExec(queryContext: QueryContext,
                                            dispatcher: PlanDispatcher,
                                            children: Seq[ExecPlan]) extends DistConcatExec
                                            with LabelCardinalityExecPlan {

  import scala.collection.mutable.{Map => MutableMap}

  private def mapConsumer(sketchMap: MutableMap[ZeroCopyUTF8String, CpcSketch]) = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      val key = new ZeroCopyUTF8String(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
      val numBytes = UTF8StringMedium.numBytes(valueBase, valueOffset)
      val sketchBytes = ZeroCopyUTF8String(
        valueBase.asInstanceOf[Array[Byte]], valueOffset.toInt + 2 - UnsafeUtils.arayOffset, numBytes).bytes
      val newSketch = sketchMap.get(key) match {
        case Some(existing: CpcSketch) =>
          // TODO: Again, hardcoding, lgK need this needs to be configurable
          val union = new CpcUnion(logK)
          union.update(existing)
          union.update(CpcSketch.heapify(sketchBytes))
          union.getResult

        case None => CpcSketch.heapify(sketchBytes)
      }
      sketchMap += (key -> newSketch)
    }
  }

  override protected def compose(childResponses: Observable[(QueryResponse, Int)],
                                 firstSchema: Task[ResultSchema],
                                 querySession: QuerySession): Observable[RangeVector] = {
      qLogger.debug(s"LabelCardinalityDistConcatExec: Concatenating results")
      val taskOfResults = childResponses.map {
        case (QueryResult(_, _, result, _, _, _), _) => result
        case (QueryError(_, _, ex), _)         => throw ex
      }.foldLeftL(scala.collection.mutable.Map.empty[ZeroCopyUTF8String, CpcSketch])
      { case (metadataResult, rv) =>
          rv.head.rows().foreach { rowReader =>
            val binaryRowReader = rowReader.asInstanceOf[BinaryRecordRowReader]
            rv.head match {
              case srv: SerializedRangeVector =>
                srv.schema.consumeMapItems(
                  binaryRowReader.recordBase,
                  binaryRowReader.recordOffset,
                  index = 0,
                  mapConsumer(metadataResult))
              case _ => throw new UnsupportedOperationException("Metadata query currently needs SRV results")
            }
        }
        metadataResult
      }.map (metaDataMutableMap => {
        // metaDataMutableMap is a MutableMap[ZeroCopyUTF8String, CPCSketch], we map its values to get a
        // MutableMap[ZeroCopyUTF8String, ZeroCopyUTF8String] where the value represents the sketch bytes. The toMap
        // is called to get an  ImmutableMap. This on-heap Map then needs to be converted to a range vector by getting
        // an Iterator and then using it with IteratorBackedRangeVector.
        //TODO: There is a lot of boiler plate to convert a heap based Map to RangeVector. We either need to avoid
        // using heap data structures or get a clean oneliner to achieve it.
        val labelSketchMapIterator =
          (metaDataMutableMap.mapValues(cpcSketch => ZeroCopyUTF8String(cpcSketch.toByteArray)).toMap:: Nil).toIterator
          IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
            UTF8MapIteratorRowReader(labelSketchMapIterator), None)
        }
      )
      Observable.fromTask(taskOfResults)
  }
}

final case class PartKeysExec(queryContext: QueryContext,
                              dispatcher: PlanDispatcher,
                              dataset: DatasetRef,
                              shard: Int,
                              filters: Seq[ColumnFilter],
                              fetchFirstLastSampleTimes: Boolean,
                              start: Long,
                              end: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = source match {
      case memStore: MemStore =>
        val response = memStore.partKeysWithFilters(dataset, shard, filters,
          fetchFirstLastSampleTimes, end, start, queryContext.plannerParams.sampleLimit)
        Observable.now(IteratorBackedRangeVector(
          new CustomRangeVectorKey(Map.empty), UTF8MapIteratorRowReader(response), None))
      case _ => Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("Labels", ColumnType.MapColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, limit=${queryContext.plannerParams.sampleLimit}"
}

final case class LabelValuesExec(queryContext: QueryContext,
                                 dispatcher: PlanDispatcher,
                                 dataset: DatasetRef,
                                 shard: Int,
                                 filters: Seq[ColumnFilter],
                                 columns: Seq[String],
                                 startMs: Long,
                                 endMs: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = if (source.isInstanceOf[MemStore]) {
      val memStore = source.asInstanceOf[MemStore]
      val response = filters.isEmpty match {
        // retrieves label values for a single label - no column filter
        case true if columns.size == 1 => memStore.labelValues(dataset, shard, columns.head, queryContext.
          plannerParams.sampleLimit).map(termInfo => Map(columns.head.utf8 -> termInfo.term)).toIterator
        case true => throw new BadQueryException("either label name is missing " +
          "or there are multiple label names without filter")
        case false => memStore.labelValuesWithFilters(dataset, shard, filters, columns, endMs, startMs,
          queryContext.plannerParams.sampleLimit)
      }
      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        UTF8MapIteratorRowReader(response), None))
    } else {
      Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("Labels", ColumnType.MapColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, col=$columns, limit=${queryContext.plannerParams.sampleLimit}," +
    s" startMs=$startMs, endMs=$endMs"
}

final case class LabelCardinalityExec(queryContext: QueryContext,
                                 dispatcher: PlanDispatcher,
                                 dataset: DatasetRef,
                                 shard: Int,
                                 filters: Seq[ColumnFilter],
                                 startMs: Long,
                                 endMs: Long) extends LeafExecPlan with LabelCardinalityExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = source match {
      case ms: MemStore =>
        // TODO: Do we need to check for presence of all three, _ws_, _ns_ and _metric_?
        // TODO: What should be the limit, where to configure?
        // TODO: We don't need to allocate intermediate Map and create an Iterator of Map, instead we can get raw byte
        //  sequences and operate directly with it to create the final data structures we need
        val partKeysMap = ms.partKeysWithFilters(dataset, shard, filters, fetchFirstLastSampleTimes = false,
          endMs, startMs, limit = 1000000)

        val metadataResult = scala.collection.mutable.Map.empty[String, CpcSketch]
        partKeysMap.foreach { rv =>
          rv.foreach {
            case (labelName, labelValue) =>
              metadataResult.getOrElseUpdate(labelName.toString, new CpcSketch(logK)).update(labelValue.toString)
          }
        }
        val sketchMapIterator = (metadataResult.map {
          case (label, cpcSketch) => (ZeroCopyUTF8String(label), ZeroCopyUTF8String(cpcSketch.toByteArray))
        }.toMap :: Nil).toIterator
        Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
          UTF8MapIteratorRowReader(sketchMapIterator), None))
      case _ => Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("Labels", ColumnType.MapColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, limit=${queryContext.plannerParams.sampleLimit}," +
    s" startMs=$startMs, endMs=$endMs"
}

final case object MetricCardTopkExec {
  // TODO(a_theimer): unsure what to set this to; also should be client-defined
  val MAX_ITEMSKETCH_MAP_SIZE = 16;
}

final case class MetricCardTopkExec(queryContext: QueryContext,
                                    dispatcher: PlanDispatcher,
                                    dataset: DatasetRef,
                                    shard: Int,
                                    shardKeyPrefix: Seq[String],
                                    k: Int,
                                    startMs: Long,
                                    endMs: Long) extends LeafExecPlan {
  override def enforceLimit: Boolean = false

  private def partKeysToRvObservable(resp: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]])
              : Observable[IteratorBackedRangeVector] = {

    val METRIC_KEY = "_metric_".utf8

    // TODO(a_theimer): this can be removed if MemStore.partKeysWithFilters returns only unique partKeys
    // keep track of keys we've seen.
    val partKeySeen = new mutable.HashSet[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
    resp.foreach(pkey => partKeySeen.add(pkey))

    // TODO(a_theimer): don't do any this real-time?

    val sketch = new ItemsSketch[ZeroCopyUTF8String](MetricCardTopkExec.MAX_ITEMSKETCH_MAP_SIZE)
    partKeySeen.foreach(pkey => sketch.update(pkey(METRIC_KEY)))

    val serSketch = ZeroCopyUTF8String(sketch.toByteArray(new ZeroCopyUtf8SerDe))

    val it = Seq(SingleValueRowReader(serSketch)).iterator
    Observable.now(IteratorBackedRangeVector(
      new CustomRangeVectorKey(Map.empty), NoCloseCursor(it), None))
  }

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    // TODO(a_theimer): async most of the below?

    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val fetchFirstLastSampleTimes = false
    val rvs = source match {
      case tsMemStore: TimeSeriesMemStore =>
        // find out which metrics have the top k cardinalities
        val cardResponse = tsMemStore.topKCardinality(dataset, Seq(shard), shardKeyPrefix, k)

        // get all the partKeys for each metric
        val partKeyResponse = cardResponse.flatMap { card =>
          val filters = Seq(
            ColumnFilter("_ws_", Filter.Equals(shardKeyPrefix(0))),
            ColumnFilter("_ns_", Filter.Equals(shardKeyPrefix(1))),
            ColumnFilter("_metric_", Filter.Equals(card.childName))
          )
          tsMemStore.partKeysWithFilters(dataset, shard, filters,
            fetchFirstLastSampleTimes, endMs, startMs, queryContext.plannerParams.sampleLimit)
        }.iterator

        // Build the count-min sketch.
        // For each unique partKey, increment the _metric_'s counters in the sketch.
        partKeysToRvObservable(partKeyResponse)

      case other =>
        Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("MetricTopkSketch", ColumnType.StringColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  // TODO(a_theimer): just replaced filters string with $shardKeyPrefix
  def args: String = s"shard=$shard, filters=$shardKeyPrefix, limit=${queryContext.plannerParams.sampleLimit}," +
    s" startMs=$startMs, endMs=$endMs"
}

final case class LabelNamesExec(queryContext: QueryContext,
                                 dispatcher: PlanDispatcher,
                                 dataset: DatasetRef,
                                 shard: Int,
                                 filters: Seq[ColumnFilter],
                                 startMs: Long,
                                 endMs: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = if (source.isInstanceOf[MemStore]) {
      val memStore = source.asInstanceOf[MemStore]
      val response = memStore.labelNames(dataset, shard, filters, endMs, startMs)

      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        NoCloseCursor(StringArrayRowReader(response)), None))
    } else {
      Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("Labels", ColumnType.StringColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, limit=5," +
    s" startMs=$startMs, endMs=$endMs"
}
