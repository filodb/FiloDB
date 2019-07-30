package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{MemStore, PartKeyRowReader}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.{UTF8MapIteratorRowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.Query.qLogger


final case class PartKeysDistConcatExec(id: String,
                                      dispatcher: PlanDispatcher,
                                      children: Seq[ExecPlan]) extends NonLeafExecPlan {

  require(!children.isEmpty)

  override def enforceLimit: Boolean = false

  /**
    * Schema of the RangeVectors returned by compose() method
    */
  override protected def schemaOfCompose(dataset: Dataset): ResultSchema = children.head.schema(dataset)

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = ""

  /**
    * Compose the sub-query/leaf results here.
    */
  protected def compose(dataset: Dataset,
                        childResponses: Observable[(QueryResponse, Int)],
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

final case class LabelValuesDistConcatExec(id: String,
                                    dispatcher: PlanDispatcher,
                                    children: Seq[ExecPlan]) extends NonLeafExecPlan {

  require(!children.isEmpty)

  override def enforceLimit: Boolean = false

  /**
    * Schema of the RangeVectors returned by compose() method
    */
  override protected def schemaOfCompose(dataset: Dataset): ResultSchema = children.head.schema(dataset)

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = ""

  /**
    * Compose the sub-query/leaf results here.
    */
  protected def compose(dataset: Dataset,
                        childResponses: Observable[(QueryResponse, Int)],
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
              case srv: SerializableRangeVector =>
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

final case class PartKeysExec(id: String,
                              submitTime: Long,
                              limit: Int,
                              dispatcher: PlanDispatcher,
                              dataset: DatasetRef,
                              shard: Int,
                              filters: Seq[ColumnFilter],
                              start: Long,
                              end: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  protected def doExecute(source: ChunkSource,
                          dataset1: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {


    if (source.isInstanceOf[MemStore]) {
      val memStore = source.asInstanceOf[MemStore]
      val response = memStore.partKeysWithFilters(dataset, shard, filters, end, start, limit)
      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty), new PartKeyRowReader(response)))
    } else {
      Observable.empty
    }
  }

  def args: String = s"shard=$shard, filters=$filters, limit=$limit"

  /**
    * Schema of QueryResponse returned by running execute()
    */
  def schemaOfDoExecute(dataset: Dataset): ResultSchema = {
    val partKeyResultSchema = new ResultSchema(dataset.partitionColumns.map(c=>ColumnInfo(c.name, c.columnType)),
                                               dataset.partitionColumns.length)
    new ResultSchema(Seq(ColumnInfo("TimeSeries", ColumnType.BinaryRecordColumn)), 1,
      Map(0->partKeyResultSchema.columns.map(c => ColumnInfo(c.name, c.colType))))
  }

}

final case class  LabelValuesExec(id: String,
                                  submitTime: Long,
                                  limit: Int,
                                  dispatcher: PlanDispatcher,
                                  dataset: DatasetRef,
                                  shard: Int,
                                  filters: Seq[ColumnFilter],
                                  columns: Seq[String],
                                  lookBackInMillis: Long) extends LeafExecPlan {

  override def enforceLimit: Boolean = false

  protected def doExecute(source: ChunkSource,
                          dataset1: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {

    if (source.isInstanceOf[MemStore]) {
      val memStore = source.asInstanceOf[MemStore]
      val curr = System.currentTimeMillis()
      val end = curr - curr % 1000 // round to the floor second
      val start = end - lookBackInMillis
      val response = filters.isEmpty match {
        // retrieves label values for a single label - no column filter
        case true if (columns.size == 1) => memStore.labelValues(dataset, shard, columns.head, limit)
          .map(termInfo => Map(columns.head.utf8 -> termInfo.term))
          .toIterator
        case true => throw new BadQueryException("either label name is missing " +
          "or there are multiple label names without filter")
        case false => memStore.labelValuesWithFilters(dataset, shard, filters, columns, end, start, limit)
      }
      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new UTF8MapIteratorRowReader(response)))
    } else {
      Observable.empty
    }
  }

  def args: String = s"shard=$shard, filters=$filters, col=$columns, limit=$limit, lookBackInMillis=$lookBackInMillis"

  /**
    * Schema of QueryResponse returned by running execute()
    */
  def schemaOfDoExecute(dataset: Dataset): ResultSchema =
    new ResultSchema(Seq(ColumnInfo("Labels", ColumnType.MapColumn)), 1)
}
