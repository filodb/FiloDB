package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.memstore.{MemStore, PartKeyRowReader}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.{ZCUTF8IteratorRowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.Query.qLogger

final case class PartKeysDistConcatExec(id: String,
                                      dispatcher: PlanDispatcher,
                                      children: Seq[ExecPlan]) extends NonLeafExecPlan {

  require(!children.isEmpty)

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
  override protected def compose(childResponses: Observable[QueryResponse], queryConfig: QueryConfig):
      Observable[RangeVector] = {

    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case QueryResult(_, _, result) => result
      case QueryError(_, ex)         => throw ex
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
  override protected def compose(childResponses: Observable[QueryResponse], queryConfig: QueryConfig):
  Observable[RangeVector] = {

    qLogger.debug(s"NonLeafMetadataExecPlan: Concatenating results")
    val taskOfResults = childResponses.map {
      case QueryResult(_, _, result) => result
      case QueryError(_, ex)         => throw ex
    }.toListL.map { resp =>
      var metadataResult = Seq.empty[ZeroCopyUTF8String]
      resp.foreach(rv => {
        metadataResult ++= rv(0).rows.toSeq.map(rowReader => rowReader.filoUTF8String(0))
      })
      //distinct -> result may have duplicates in case of labelValues
      IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new ZCUTF8IteratorRowReader(metadataResult.distinct.toIterator))
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
                                  column: String,
                                  lookBackInMillis: Long) extends LeafExecPlan {

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
        case true => memStore.indexValues(dataset, shard, column, limit).map(_.term).toIterator
        case false => memStore.indexValuesWithFilters(dataset, shard, filters, column, end, start, limit)
      }
      Observable.now(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new ZCUTF8IteratorRowReader(response)))
    } else {
      Observable.empty
    }
  }

  def args: String = s"shard=$shard, filters=$filters, col=$column, limit=$limit, lookBackInMillis=$lookBackInMillis"

  /**
    * Schema of QueryResponse returned by running execute()
    */
  def schemaOfDoExecute(dataset: Dataset): ResultSchema =
    new ResultSchema(Seq(ColumnInfo(column, ColumnType.StringColumn)), 1)
}
