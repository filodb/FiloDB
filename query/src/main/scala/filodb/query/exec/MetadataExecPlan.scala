package filodb.query.exec

import scala.collection.mutable

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.{BinaryRecordRowReader, MapItemConsumer}
import filodb.core.memstore.{TimeSeriesMemStore, TimeSeriesStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.store.ChunkSource
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format._
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger

trait MetadataDistConcatExec extends NonLeafExecPlan {

  require(children.nonEmpty)

  override def enforceSampleLimit: Boolean = false

  override val maxRecordContainerSize: Int = 128 * 1024

  /**
   * Args to use for the ExecPlan for printTree purposes only.
   * DO NOT change to a val. Increases heap usage.
   */
  override protected def args: String = ""

  /**
    * Compose the sub-query/leaf results here.
    */
  protected def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map(_._1.result).toListL.map { resp =>
      val metadataResult = scala.collection.mutable.Set.empty[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
      resp.filter(_.nonEmpty).foreach { rv =>
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

/**
  * Aggregates output from TsCardExec.
  * When the aggregated data contains MAX_RESULT_SIZE (names -> cardinalities) pairs,
  *   pairs with previously-unseen names will be counted into an overflow bucket
  *   with group OVERFLOW_GROUP.
  */
final case class TsCardReduceExec(queryContext: QueryContext,
                                  dispatcher: PlanDispatcher,
                                  children: Seq[ExecPlan]) extends NonLeafExecPlan {
  import TsCardExec._

  override protected def args: String = ""

  private def mapFold(acc: mutable.HashMap[ZeroCopyUTF8String, CardCounts], rv: RangeVector):
      mutable.HashMap[ZeroCopyUTF8String, CardCounts] = {
    rv.rows().foreach{ r =>
      val data = RowData.fromRowReader(r)
      val accCountsOpt = acc.get(data.group)
      // Check if we either (1) won't increase the size or (2) have enough room for another.
      // Accordingly retrieve the key to update and the counts to increment.
      val (groupKey, accCounts) = if (accCountsOpt.nonEmpty || acc.size < MAX_RESULT_SIZE) {
        (data.group, accCountsOpt.getOrElse(CardCounts(0, 0)))
      } else {
        (OVERFLOW_GROUP, acc.getOrElseUpdate(OVERFLOW_GROUP, CardCounts(0, 0)))
      }
      acc.update(groupKey, accCounts.add(data.counts))
    }
    acc
  }

  override protected def compose(childResponses: Observable[(QueryResult, Int)],
                                 firstSchema: Task[ResultSchema],
                                 querySession: QuerySession): Observable[RangeVector] = {
    val taskOfResults = childResponses.flatMap(res => Observable.fromIterable(res._1.result))
      .foldLeftL(new mutable.HashMap[ZeroCopyUTF8String, CardCounts])(mapFold)
      .map{ aggMap =>
        val it = aggMap.toSeq.sortBy(-_._2.total).map{ case (group, counts) =>
          CardRowReader(group, counts)
        }.iterator
        IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), NoCloseCursor(it), None)
      }
    Observable.fromTask(taskOfResults)
  }
}

final case class LabelValuesDistConcatExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           children: Seq[ExecPlan]) extends MetadataDistConcatExec {
  /**
   * Compose the sub-query/leaf results here.
   */
  override final def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case (QueryResult(_, schema, result, _, _, _), _) => (schema, result)
    }.toListL.map { resp =>
      val colType = resp.head._1.columns.head.colType
      if (colType == MapColumn) {
        val metadataResult = scala.collection.mutable.Set.empty[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
        resp.foreach { result =>
          val rv = result._2
          metadataResult ++= transformRVs(rv.head, colType)
            .asInstanceOf[Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]]
        }
        IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
          UTF8MapIteratorRowReader(metadataResult.toIterator), None)
      } else {
        val metadataResult = scala.collection.mutable.Set.empty[String]
        resp.foreach { result =>
          val rv = result._2
          metadataResult ++= transformRVs(rv.head, colType)
            .asInstanceOf[Iterator[String]]
        }
        IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
          NoCloseCursor(StringArrayRowReader(metadataResult.toSeq)), None)
      }
    }
    Observable.fromTask(taskOfResults)
  }

  private def transformRVs(rv: RangeVector, colType: ColumnType): Iterator[Any] = {
    val metadataResult = rv.rows.map { rowReader =>
      val binaryRowReader = rowReader.asInstanceOf[BinaryRecordRowReader]
      rv match {
        case srv: SerializedRangeVector if colType == MapColumn =>
          srv.schema.toStringPairs (binaryRowReader.recordBase, binaryRowReader.recordOffset)
            .map (pair => pair._1.utf8 -> pair._2.utf8).toMap
        case srv: SerializedRangeVector if colType == StringColumn =>
          srv.schema.toStringPairs (binaryRowReader.recordBase, binaryRowReader.recordOffset)
            .map (_._2).head
        case _ => throw new UnsupportedOperationException("Metadata query currently needs SRV results")
      }
    }
    metadataResult
  }

}

final class LabelCardinalityPresenter(val funcParams: Seq[FuncArgs]  = Nil) extends RangeVectorTransformer {

  override def apply(source: Observable[RangeVector],
                     querySession: QuerySession,
                     limit: Int,
                     sourceSchema: ResultSchema,
                     paramsResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    source.filter(!_.rows().isEmpty).map(rv => {
      val x = rv.rows().next()
      // TODO: We expect only one column to be a map, pattern matching does not work, is there better way?
      val sketchMap = x.getAny(columnNo = 0).asInstanceOf[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
          val sketchMapIterator = (sketchMap.mapValues {
            sketch => ZeroCopyUTF8String(Math.round(CpcSketch.heapify(sketch.bytes).getEstimate).toInt.toString)}
            :: Nil).toIterator
      IteratorBackedRangeVector(rv.key, UTF8MapIteratorRowReader(sketchMapIterator), None)
      })
  }

  override protected[exec] def args: String = s"LabelCardinalityPresenter"
}

final case class LabelNamesDistConcatExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           children: Seq[ExecPlan]) extends MetadataDistConcatExec {
  /**
   * Pick first non empty result from child.
   */
  override final def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    childResponses.map(_._1.result)
                  .filter(s => s.nonEmpty && s.head.numRows.getOrElse(1) > 0)
                  .head.map(_.head)
  }
}

trait LabelCardinalityExecPlan {
  /**
   * Parameter deciding the sketch size to be used for approximating cardinality
   */
  val logK = 10
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

  override protected def compose(childResponses: Observable[(QueryResult, Int)],
                                 firstSchema: Task[ResultSchema],
                                 querySession: QuerySession): Observable[RangeVector] = {
      qLogger.debug(s"LabelCardinalityDistConcatExec: Concatenating results")
      val taskOfResults: Task[Observable[RangeVector]] = childResponses.map(_._1.result)
        .filter(_.nonEmpty)
        .foldLeftL(MutableMap.empty[RangeVectorKey, MutableMap[ZeroCopyUTF8String, CpcSketch]])
      { case (metadataResult, rv) =>
          val rangeVector = rv.head
          val key = rangeVector.key
          val sketchMap = metadataResult.getOrElseUpdate(key, MutableMap.empty[ZeroCopyUTF8String, CpcSketch])
          rangeVector.rows().foreach { rowReader =>
            val binaryRowReader = rowReader.asInstanceOf[BinaryRecordRowReader]
            rv.head match {
              case srv: SerializedRangeVector =>
                srv.schema.consumeMapItems(binaryRowReader.recordBase, binaryRowReader.recordOffset, index = 0,
                  mapConsumer(sketchMap))
              case _ => throw new UnsupportedOperationException("Metadata query currently needs SRV results")
            }
          }
          metadataResult
      }.map (metaDataMutableMap => {
          // metaDataMutableMap is a Map of [RangeVectorKey, MutableMap[ZeroCopyUTF8String, CpcSketch]]
          // Since the key query is specific to one ws/ns/metric, we see no more than one entry in the Map
          // The value of the map is a MutableMap[ZeroCopyUTF8String, CPCSketch], we map its values to get a
          // MutableMap[ZeroCopyUTF8String, ZeroCopyUTF8String] where the value represents the sketch bytes. The toMap
          // is called to get an  ImmutableMap. This on-heap Map then needs to be converted to a range vector by getting
          // an Iterator and then using it with IteratorBackedRangeVector.
          //TODO: There is a lot of boiler plate to convert a heap based Map to RangeVector. We either need to avoid
          // using heap data structures or get a clean oneliner to achieve it.
          if (metaDataMutableMap.isEmpty) {
            Observable.now(IteratorBackedRangeVector(CustomRangeVectorKey(Map.empty),
              UTF8MapIteratorRowReader(List.empty.toIterator), None))
          } else {
            val x = for ((key, sketchMap) <- metaDataMutableMap) yield {
              val labelSketchMapIterator =
                Seq(sketchMap.mapValues(cpcSketch => ZeroCopyUTF8String(cpcSketch.toByteArray)).toMap).toIterator
              IteratorBackedRangeVector(key, UTF8MapIteratorRowReader(labelSketchMapIterator), None)
            }
            Observable.fromIterable(x)
          }
        }
      )
      Observable.fromTask(taskOfResults).flatten
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

  override def enforceSampleLimit: Boolean = false

  /**
   * Override the max record container size created for PartKeysExec to 8K instead of default 4K
   */
  override val maxRecordContainerSize: Int = 128 * 1024

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = source match {
      case memStore: TimeSeriesStore =>
        val response = memStore.partKeysWithFilters(dataset, shard, filters,
          fetchFirstLastSampleTimes, end, start, queryContext.plannerParams.sampleLimit)
        Observable.now(IteratorBackedRangeVector(
          new CustomRangeVectorKey(Map.empty), UTF8MapIteratorRowReader(response), None))
      case _ => Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("metadataMap", ColumnType.MapColumn)), 1)
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

  override def enforceSampleLimit: Boolean = false

  override val maxRecordContainerSize: Int = 128 * 1024

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val execResult = if (source.isInstanceOf[TimeSeriesStore]) {
      val memStore = source.asInstanceOf[TimeSeriesStore]
      filters.isEmpty match {
        // retrieves label values for a single label - no column filter
        case true if (columns.size == 1) =>
          val labels = memStore.labelValues(dataset, shard, columns.head,
            queryContext.plannerParams.sampleLimit).map(_.term.toString)
          val resp = Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
            StringArrayRowReader(labels), None))
          val sch = if (labels.isEmpty) ResultSchema.empty
                    else ResultSchema(Seq(ColumnInfo("Labels", ColumnType.StringColumn)), 1)
          ExecResult(resp, Task.eval(sch))
        case true => throw new BadQueryException("either label name is missing " +
          "or there are multiple label names without filter")
        case false if (columns.size == 1) =>
          val labelsIter = memStore.singleLabelValueWithFilters(dataset, shard, filters, columns.head, endMs, startMs,
            querySession, queryContext.plannerParams.sampleLimit)
          val resp = Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
            UTF8StringRowReader(labelsIter), None))
          val sch = if (labelsIter.isEmpty) ResultSchema.empty
            else ResultSchema(Seq(ColumnInfo("Labels", ColumnType.StringColumn)), 1)
          ExecResult(resp, Task.eval(sch))
        case false =>
          val metadataMap = memStore.labelValuesWithFilters(dataset, shard, filters, columns, endMs, startMs,
            querySession, queryContext.plannerParams.sampleLimit)
          val resp = Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
            UTF8MapIteratorRowReader(metadataMap), None))
          val sch = if (metadataMap.isEmpty) ResultSchema.empty
                    else ResultSchema(Seq(ColumnInfo("metadataMap", ColumnType.MapColumn)), 1)
          ExecResult(resp, Task.eval(sch))
      }
    } else {
      val resp = Observable.empty
      val sch = ResultSchema.empty
      ExecResult(resp, Task.eval(sch))
    }
    execResult
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

  override def enforceSampleLimit: Boolean = false

  // scalastyle:off
  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = source match {
      case memstore: TimeSeriesStore =>
        // first get the list of label names, then fetch each label value and add to CPC sketch
        val labelNames = memstore.labelNames(dataset, shard, filters, endMs, startMs)
        if (labelNames.nonEmpty) {
          val sketchMap = scala.collection.mutable.Map[String, CpcSketch]()
          labelNames.foreach { case label =>
            // GOTCHA: This approach will not catch cardinality of labels which are disabled for faceting
            // since their value lengths are > 1000. We expect the gateway to reject (or shorten) that data early on.
            memstore.singleLabelValueWithFilters(dataset, shard, filters, label.toString,
              endMs, startMs, querySession, 1000000).foreach { labelValue =>
              sketchMap.getOrElseUpdate(label, new CpcSketch(logK)).update(labelValue.toString)
            }
          }

          val rv = IteratorBackedRangeVector(CustomRangeVectorKey.empty,
            UTF8MapIteratorRowReader(
              Seq(sketchMap.map {
                case (label, cpcSketch) =>
                  (ZeroCopyUTF8String(label), ZeroCopyUTF8String(cpcSketch.toByteArray))
              }.toMap).toIterator),
            None)
          Observable.now(rv)
        } else {
          Observable.empty
        }
      case _ => Observable.empty
    }
    val sch = ResultSchema(Seq(ColumnInfo("metadataMap", ColumnType.MapColumn)), 1)
    ExecResult(rvs, Task.eval(sch))
  }

  def args: String = s"shard=$shard, filters=$filters, limit=${queryContext.plannerParams.sampleLimit}," +
    s" startMs=$startMs, endMs=$endMs"
}

/**
 * Contains utilities for all TsCardinality materialize() derivatives.
 */
final case object TsCardExec {
  import filodb.core.memstore.ratelimit.CardinalityStore

  // results from all TsCardinality derivatives are clipped to this size
  val MAX_RESULT_SIZE = CardinalityStore.MAX_RESULT_SIZE

  // row name assigned to overflow counts
  val OVERFLOW_GROUP = prefixToGroup(CardinalityStore.OVERFLOW_PREFIX)

  val PREFIX_DELIM = ","

  val RESULT_SCHEMA = ResultSchema(Seq(ColumnInfo("group", ColumnType.StringColumn),
                                       ColumnInfo("active", ColumnType.IntColumn),
                                       ColumnInfo("total", ColumnType.IntColumn)), 1)

  /**
   * Convert a shard key prefix to a row's group name.
   */
  def prefixToGroup(prefix: Seq[String]): ZeroCopyUTF8String = {
    // just concat the prefix together with a single char delimiter
    prefix.mkString(PREFIX_DELIM).utf8
  }

  case class CardCounts(active: Int, total: Int) {
    if (total < active) {
      qLogger.warn(s"CardCounts created with total < active; total: $total, active: $active")
    }
    def add(other: CardCounts): CardCounts = {
      CardCounts(active + other.active,
                 total + other.total)
    }
  }

  case class CardRowReader(group: ZeroCopyUTF8String, counts: CardCounts) extends RowReader {
    override def notNull(columnNo: Int): Boolean = ???
    override def getBoolean(columnNo: Int): Boolean = ???
    override def getInt(columnNo: Int): Int = columnNo match {
      case 1 => counts.active
      case 2 => counts.total
      case _ => throw new IllegalArgumentException(s"illegal getInt columnNo: $columnNo")
    }
    override def getLong(columnNo: Int): Long = ???
    override def getDouble(columnNo: Int): Double = ???
    override def getFloat(columnNo: Int): Float = ???
    override def getString(columnNo: Int): String = {
      throw new RuntimeException("for group: call getAny and cast to ZeroCopyUtf8String")
    }
    override def getAny(columnNo: Int): Any = columnNo match {
      case 0 => group
      case _ => throw new IllegalArgumentException(s"illegal getAny columnNo: $columnNo")
    }
    override def getBlobBase(columnNo: Int): Any = ???
    override def getBlobOffset(columnNo: Int): Long = ???
    override def getBlobNumBytes(columnNo: Int): Int = ???
  }

  /**
   * Convenience class for interpreting RowReader data.
   */
  case class RowData(group: ZeroCopyUTF8String, counts: CardCounts)
  object RowData {
    def fromRowReader(rr: RowReader): RowData = {
      val group = rr.getAny(0).asInstanceOf[ZeroCopyUTF8String]
      val counts = CardCounts(rr.getInt(1),
                              rr.getInt(2))
      RowData(group, counts)
    }
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
                            numGroupByFields: Int) extends LeafExecPlan with StrictLogging {
  require(numGroupByFields >= 1,
    "numGroupByFields must be positive")
  require(numGroupByFields >= shardKeyPrefix.size,
    s"numGroupByFields ($numGroupByFields) must indicate at least as many " +
    s"fields as shardKeyPrefix.size (${shardKeyPrefix.size})")

  override def enforceSampleLimit: Boolean = false

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
          val cards = tsMemStore.scanTsCardinalities(
            dataset, Seq(shard), shardKeyPrefix, numGroupByFields)
          val it = cards.map{ card =>
            CardRowReader(prefixToGroup(card.prefix),
                          CardCounts(card.activeTsCount, card.tsCount))
            }.iterator
          IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), NoCloseCursor(it), None)
        }
      case other =>
        Observable.empty
    }
    ExecResult(rvs, Task.eval(RESULT_SCHEMA))
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

  override def enforceSampleLimit: Boolean = false

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    val rvs = if (source.isInstanceOf[TimeSeriesStore]) {
      val memStore = source.asInstanceOf[TimeSeriesStore]
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
