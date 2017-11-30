package filodb.core.memstore

import filodb.core.metadata.Dataset
import filodb.memory.format.{RoutingRowReader, RowReader, SchemaRoutingRowReader, SchemaRowReader}
import filodb.memory.format.RowReader.TypedFieldExtractor

/**
 * A record for ingesting into a MemStore.
 *
 * partition key is separated from the other data columns, as the partition key does not need
 * to be stored, but is needed for routing and other purposes... = less extraction time
 * This allows a partition key to be reused across multiple records also, for efficiency :)
 */
final case class IngestRecord(partition: SchemaRowReader, data: RowReader, offset: Long)

object IngestRecord {
  // Creates an IngestRecord from a reader spanning a single entire record, such as that from a CSV file
  def apply(routing: IngestRouting, reader: RowReader, offset: Long): IngestRecord =
    IngestRecord(SchemaRoutingRowReader(reader, routing.partColRouting, routing.partExtractors),
                 RoutingRowReader(reader, routing.dataColRouting),
                 offset)
}

/**
 * When you ingest readers spanning entire records, which include both the data and partitioning columns
 * mixed together (ex., CSV files and Spark ingestion sources), you need a way to separate out the
 * data and partitioning columns and route them into separate readers.  This class figures out the routing
 * necessary.
 * NOTE: not all input columns may be used.
 * Create one of these for each (dataset, ingest schema) pair.
 */
final case class IngestRouting(partColRouting: Array[Int],
                               dataColRouting: Array[Int],
                               partExtractors: Array[TypedFieldExtractor[_]]) {
  override def toString: String =
    s"IngestRouting(partRouting=${partColRouting.toList} dataColRouting=${dataColRouting.toList})"
}

final case class MissingPartitionColumns(cols: Seq[String]) extends
  Exception(s"Missing partition columns ${cols.mkString(", ")}")
final case class MissingDataColumns(cols: Seq[String]) extends
  Exception(s"Missing data columns ${cols.mkString(", ")}")

object IngestRouting {
  /**
   * Creates an IngestRouting given an input record schema of column names and a given dataset definition.
   * @param dataset the Dataset to ingest into
   * @param columnNames the names of the input record columns in order.  Must match the dataset definition.
   * @return an IngestRouting.  Missing*Columns may be thrown if some columnNames are missing.
   */
  def apply(dataset: Dataset, columnNames: Seq[String]): IngestRouting = {
    val partExtractors = dataset.partitionColumns.map(_.extractor).toArray
    val partIndices = dataset.partitionColumns.map(c => columnNames.indexOf(c.name))
    val missingPartCols = partIndices.zipWithIndex.filter(_._1 < 0).map(_._2)
    if (missingPartCols.nonEmpty)
      throw MissingPartitionColumns(missingPartCols.map(dataset.partitionColumns).map(_.name))

    val dataIndices = dataset.dataColumns.map(c => columnNames.indexOf(c.name))
    val missingDataCols = dataIndices.zipWithIndex.filter(_._1 < 0).map(_._2)
    if (missingDataCols.nonEmpty)
      throw MissingDataColumns(missingDataCols.map(dataset.dataColumns).map(_.name))

    IngestRouting(partIndices.toArray, dataIndices.toArray, partExtractors)
  }
}
