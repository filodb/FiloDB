package filodb.query.exec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.{BinaryRecordRowReader, MapItemConsumer}
import filodb.core.memstore.{MemStore, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.store.ChunkSource
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{SeqRowReader, SingleValueRowReader, StringArrayRowReader,
                             UnsafeUtils, UTF8MapIteratorRowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.TsCardExec.{ADD_INACTIVE, MAX_RESPONSE_SIZE}

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

case object SerializeUtils {
  def serialize(obj: Any) : Array[Byte] = {
    val byte_os = new ByteArrayOutputStream()
    val obj_os = new ObjectOutputStream(byte_os)
    obj_os.writeObject(obj)
    val res = byte_os.toByteArray
    byte_os.close()
    obj_os.close()
    res
  }
  def deSerialize[T](bytes: Array[Byte]): T = {
    val byte_is = new ByteArrayInputStream(bytes)
    val obj_is = new ObjectInputStream(byte_is)
    val res = obj_is.readObject().asInstanceOf[T]
    byte_is.close()
    obj_is.close()
    res
  }
}

final case class PartKeysDistConcatExec(queryContext: QueryContext,
                                        dispatcher: PlanDispatcher,
                                        children: Seq[ExecPlan]) extends MetadataDistConcatExec

/**
  * Aggregates output from TsCardExec.
  * When the aggregated data contains MAX_RESPONSE_SIZE (names -> cardinalities) pairs,
  *   pairs with previously-unseen names will be counted into an overflow bucket.
  */
final case class TsCardReduceExec(queryContext: QueryContext,
                                  dispatcher: PlanDispatcher,
                                  children: Seq[ExecPlan]) extends NonLeafExecPlan {
  import TsCardExec._

  override protected def args: String = ""

  private def mapFold(acc: mutable.HashMap[Seq[ZeroCopyUTF8String], CardCounts], rv: RangeVector):
      mutable.HashMap[Seq[ZeroCopyUTF8String], CardCounts] = {
    rv.rows().foreach{ r =>
      val mapSer = r.getAny(0).asInstanceOf[ZeroCopyUTF8String].bytes
      val map = SerializeUtils.deSerialize[Map[Seq[ZeroCopyUTF8String], CardCounts]](mapSer)
      map.foreach{ case (names, counts) =>
        val accCountsOpt = acc.get(names)
        // check if we either (1) won't increase the size or (2) have enough room for another
        val accCounts = if (accCountsOpt.nonEmpty || acc.size < MAX_RESPONSE_SIZE) {
          accCountsOpt.getOrElse(CardCounts(0, 0))
        } else {
          acc.getOrElseUpdate(names.map(_ => OVERFLOW_NAME).toSeq, CardCounts(0, 0))
        }
        val sumCounts = CardCounts(accCounts.active + counts.active,
                                   accCounts.total + counts.total)
        acc.update(names, sumCounts)
      }
    }
    acc
  }

  override protected def compose(childResponses: Observable[(QueryResponse, Int)],
                                 firstSchema: Task[ResultSchema],
                                 querySession: QuerySession): Observable[RangeVector] = {
    val taskOfResults = childResponses.map {
      case (QueryResult(_, _, result, _, _, _), _) => Observable.fromIterable(result)
      case (QueryError(_, _, ex), _)         => throw ex
    }.flatten
      .foldLeftL(new mutable.HashMap[Seq[ZeroCopyUTF8String], CardCounts])(mapFold)
      .map{ aggMap =>
        val serMap = ZeroCopyUTF8String(SerializeUtils.serialize(aggMap.toMap))  // TODO(a_theimer): toMap needed?
        val it = Seq(SingleValueRowReader(serMap)).iterator
        IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), NoCloseCursor(it), None)
      }
    Observable.fromTask(taskOfResults)
  }
}

/**
  * Dynamically packages the (prefix -> cardinalities) map into a RangeVector,
  *   where its width and column names depend upon the size of each shard key prefix.
  */
final case class TsCardPresenter(groupDepth: Int) extends RangeVectorTransformer {
  import TsCardExec._

  override def funcParams: Seq[FuncArgs] = Nil

  override protected[exec] def args: String = ""

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Seq[Observable[ScalarRangeVector]]
           ): Observable[RangeVector] = {
    source.map { rv =>
      // Note: getString() throws a ClassCastException.
      val mapSer = rv.rows.next.getAny(0).asInstanceOf[ZeroCopyUTF8String].bytes
      SerializeUtils.deSerialize[Map[Seq[ZeroCopyUTF8String], CardCounts]](mapSer)
    }.map { countMap =>
      val iter = countMap.map{ case (names, counts) =>
        val concatColValues = Seq(names, Seq(counts.active, counts.total)).flatten
        SeqRowReader(concatColValues)
      }.iterator
      IteratorBackedRangeVector(CustomRangeVectorKey(Map.empty),
        new NoCloseCursor(iter),
        None)
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // append the appropriate columns according to groupDepth
    val nameColSeq = new mutable.ArrayBuffer[String](1 + groupDepth)
    nameColSeq.append("ws")
    if (groupDepth > 0) nameColSeq.append("ns")
    if (groupDepth > 1) nameColSeq.append("metric")
    ResultSchema(
      Seq(
        nameColSeq.map(s => ColumnInfo(s, ColumnType.StringColumn)),
        Seq(ColumnInfo("Active", ColumnType.IntColumn),
            ColumnInfo("Total", ColumnType.IntColumn))
      ).flatten, 1)
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

/**
 * Contains utilities for all TsCardinality materialize() derivatives.
 */
final case object TsCardExec {
  // TODO: tune this
  val MAX_RESPONSE_SIZE = 5000

  // just to keep memStore.topKCardinality calls consistent
  val ADD_INACTIVE = true

  val OVERFLOW_NAME = "_overflow_".utf8

  // TODO(a_theimer): Int? Long?
  case class CardCounts(active: Int, total: Int) {
    require(total >= active, "total must be at least as large as active")
  }
}

/**
  * Creates a map of (prefix -> cardinalities) according to the TsCardinalities LogicalPlan.
  *   See TsCardinalities for more details.
  */
final case class TsCardExec(queryContext: QueryContext,
                            dispatcher: PlanDispatcher,
                            dataset: DatasetRef,
                            shard: Int,
                            shardKeyPrefix: Seq[String],
                            groupDepth: Int) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  /**
   * Given a shard key prefix, iterates through all child
   *   prefixes with the specified size.
   *
   * Note: next() will always return the same array instance.
   *
   * @param tsMemStore contains the shard keys over which to iterate
   * @param initPrefix shard key prefix to resolve
   * @param extendToSize the length of the prefix children to iterate through
   */
  class PrefixIterator (tsMemStore: TimeSeriesMemStore,
                        initPrefix: Seq[String],
                        extendToSize: Int) extends Iterator[Seq[String]]{

    // A queue for each set of "space" (i.e. namespace, workspace, metric) labels.
    // Spaces are stored in descending precedence, and each successive queue
    //   contains labels for the "subspace" of the label at the front of the
    //   previous queue.
    private val spaceQueues = new Array[mutable.Queue[String]](extendToSize)

    // Stores the prefix returned by next().
    private val currPrefix = new mutable.ArrayBuffer[String](extendToSize)

    private var iFirstEmptyQueue = -1

    // TODO(a_theimer): this feels gross
    initialize()

    private def initialize(): Unit = {
      // initialize each of the queues
      for (i <- 0 until extendToSize) {
        spaceQueues(i) = new mutable.Queue[String]
      }

      for (i <- 0 until initPrefix.size) {
        // Place each existing prefix label into appropriate queue.
        spaceQueues(i).enqueue(initPrefix(i))
        // Store as much of the prefix as we have available.
        currPrefix.append(initPrefix(i))
      }

      iFirstEmptyQueue = initPrefix.size

      // Need this because an empty prefix is a valid argument, and hasNext()
      //   relies on spaceQueues(0).size.
      // This results in one extra enqueueAndUpdatePrefix() call at the first next().
      enqueueAndUpdatePrefix()
    }

    /**
     * Dequeues from each queue-- in ascending precedence-- until a dequeue()
     *   does not leave a queue empty.
     *
     * This method mostly exists as setup for enqueueAndUpdatePrefix().
     */
    private def dequeue(): Unit = {
      require(spaceQueues.last.nonEmpty, "should never dequeue without full prefix")
      iFirstEmptyQueue = spaceQueues.size
      for (queue <- spaceQueues.reverseIterator) {
        queue.dequeue()
        if (queue.nonEmpty) {
          return
        }
        iFirstEmptyQueue = iFirstEmptyQueue - 1
      }
    }

    /**
     * Given the prefix defined by the front of each queue,
     *   repopulates empty queues with "subspace" labels.
     */
    private def enqueueAndUpdatePrefix(): Unit = {
      currPrefix.reduceToSize(iFirstEmptyQueue)
      // Fill each successive queue with the topKCardinality result such that
      //   its argument prefix is defined by the preceding queue front labels.
      for (iempty <- iFirstEmptyQueue until extendToSize) {
        // TODO(a_theimer): MAX_RESPONSE_SIZE works, but not exactly the same meaning
        tsMemStore.topKCardinality(dataset, Seq(shard), currPrefix,
                                   MAX_RESPONSE_SIZE, ADD_INACTIVE).foreach{ card =>
          spaceQueues(iempty).enqueue(card.childName)
        }
        // currPrefix will contain all front labels
        currPrefix.append(spaceQueues(iempty).front)
      }
      iFirstEmptyQueue = extendToSize
    }

    override def hasNext: Boolean = spaceQueues.head.nonEmpty

    override def next(): Seq[String] = {
      enqueueAndUpdatePrefix()
      dequeue() // Note: also setup for hasNext().
      currPrefix
    }
  }

  // scalastyle:off method.length
  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    import TsCardExec._

    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)

    val rvs = source match {
      case tsMemStore: TimeSeriesMemStore =>
        Observable.eval {
          val countMap = if (groupDepth == shardKeyPrefix.size - 1) {
            // shardKeyPrefix is as deep as the groupDepth; we execute only a single topKCardinality
            //   call on the "prefix's prefix", then map the prefix to its cardinality iff the
            //   "deepest" prefix label is found in the results.
            val res = tsMemStore.topKCardinality(dataset, Seq(shard),
                                                 shardKeyPrefix.dropRight(1),
                                                 MAX_RESPONSE_SIZE, ADD_INACTIVE)
              .find(card => card.childName == shardKeyPrefix.last)
            if (res.nonEmpty) {
              Map(shardKeyPrefix -> CardCounts(res.get.activeTsCount, res.get.tsCount))
            } else {
              Map()
            }
          } else {
            // groupDepth is deeper than shardKeyPrefix.
            // We will "expand" shardKeyPrefix into all of its child prefixes at groupDepth - 1,
            //   then we'll pass the children to memstore.topKCardinality and map the results.

            // The conditional exists because PrefixIterator currently
            //   needs to extend a prefix to a size > 0
            val prefixIter = if (groupDepth > 0) {
              // Note: groupDepth equals the length of the prefix to extend to (at groupDepth - 1)
              new PrefixIterator(tsMemStore, shardKeyPrefix, groupDepth)
            } else {
              Seq(Seq()).iterator
            }

            prefixIter.flatMap { prefix =>
              tsMemStore.topKCardinality(dataset, Seq(shard), prefix,
                                         MAX_RESPONSE_SIZE, ADD_INACTIVE)
                .map{ card =>
                  // Concatenate `prefix` with the least-significant name.
                  val totalPrefix = Seq(prefix, Seq(card.childName)).flatMap(_.map(_.utf8)).toSeq
                  totalPrefix -> CardCounts(card.activeTsCount, card.tsCount)
                }
            }.toMap
          }

          // serialize the map; pack it into a RangeVector
          val serMap = ZeroCopyUTF8String(SerializeUtils.serialize(countMap))

          val it = Seq(SingleValueRowReader(serMap)).iterator
          IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), NoCloseCursor(it), None)
        }
      case other =>
        Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("TsCardMap", ColumnType.StringColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }
  // scalastyle:on method.length

  def args: String = s"shard=$shard, shardKeyPrefix=$shardKeyPrefix, " +
    s"limit=${queryContext.plannerParams.sampleLimit}"
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
