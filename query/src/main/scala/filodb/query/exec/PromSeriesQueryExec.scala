package filodb.query.exec

import kamon.Kamon

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{UTF8MapIteratorRowReader, ZeroCopyUTF8String}

import filodb.query._

case class PromSeriesQueryExec(queryContext: QueryContext,
                                dispatcher: PlanDispatcher,
                                dataset: DatasetRef,
                                params: PromQlQueryParams) extends RemoteExec {
  val columns = Seq(ColumnInfo("value", ColumnType.MapColumn))

  val recSchema = SerializedRangeVector.toSchema(columns)
  val resultSchema = ResultSchema(columns, 1)
  val builder = SerializedRangeVector.newBuilder()

  override def toQueryResponse(data: Data, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val span = Kamon.spanBuilder(s"create-queryresponse-${getClass.getSimpleName}")
      .asChildOf(parentSpan)
      .tag("query-id", id)
      .start()
    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val sampleMap = samples.iterator.collect { case v: MetadataSampl =>
        v.values.map { case (k, v) => (ZeroCopyUTF8String(k), ZeroCopyUTF8String(v)) }
      }

      SerializedRangeVector(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new UTF8MapIteratorRowReader(sampleMap)), columns)
    }

    span.finish()
    QueryResult(id, resultSchema, rangeVectors)
  }
}
case class PromLabelQueryExec (queryContext: QueryContext,
                               dispatcher: PlanDispatcher,
                               dataset: DatasetRef,
                               params: PromQlQueryParams) extends RemoteExec {
  val cols = Seq(ColumnInfo("value", ColumnType.MapColumn))
  val recSchema = SerializedRangeVector.toSchema(cols)
  val resultSchema = ResultSchema(cols, 1)
  val builder = SerializedRangeVector.newBuilder()



  override def toQueryResponse(data: Data, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val span = Kamon.spanBuilder(s"create-queryresponse-${getClass.getSimpleName}")
      .asChildOf(parentSpan)
      .tag("query-id", id)
      .start()
    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val sampleMap = samples.iterator.collect { case v: MetadataSampl =>
        v.values.map { case (k, v) => (ZeroCopyUTF8String(k), ZeroCopyUTF8String(v)) }
      }

      SerializedRangeVector(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        new UTF8MapIteratorRowReader(sampleMap)), cols)
    }

    span.finish()
    QueryResult(id, resultSchema, rangeVectors)
  }
}

