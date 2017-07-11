package filodb.core.metadata

import com.typesafe.scalalogging.StrictLogging
import org.scalactic._
import org.velvia.filo.{RowReader, RoutingRowReader, SeqRowReader}

import filodb.core.{CompositeKeyType, KeyType}
import filodb.core.binaryrecord.{RecordSchema, BinaryRecord}
import filodb.core.Types._
import filodb.core._

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 * Within a partition, **row key columns** define a unique primary key for each row.
 * Records/rows are grouped by the **segment key** into segments.
 * Projections are sorted by the **segment key**.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 *
 * The Projection base class is normalized, ie it doesn't have all the information.
 */
case class Projection(id: Int,
                      dataset: DatasetRef,
                      keyColIds: Seq[ColumnId],
                      segmentColId: ColumnId,
                      reverse: Boolean = false,
                      // Nil columns means all columns
                      // Must include the rowKeyColumns and segmentColumn.
                      columns: Seq[ColumnId] = Nil) {
  def detailedString: String =
    s"Projection $id from dataset $dataset:\n" +
    s"  Key columns: ${keyColIds.mkString(", ")}\n" +
    s"  Projection columns: ${columns.mkString(", ")}"

  /**
   * Returns a new Projection with the specified database and everything else kept the same
   */
  def withDatabase(database: String): Projection =
    this.copy(dataset = this.dataset.copy(database = Some(database)))

  def withName(name: String): Projection =
    this.copy(dataset = this.dataset.copy(dataset = name))
}

/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 * It is also guaranteed to have a valid segmentColumn and dataset partition columns.
 */
case class RichProjection(projection: Projection,
                          dataset: Dataset,
                          columns: Seq[Column],
                          segmentColumn: Column, // get rid of segment*
                          segmentColIndex: Int,
                          segmentType: KeyType,
                          rowKeyColumns: Seq[Column],
                          rowKeyColIndices: Seq[Int],
                          rowKeyType: KeyType,    // get rid of this too
                          partitionColumns: Seq[Column],
                          partitionColIndices: Seq[Int]) {
  type SK = segmentType.T
  type RK = rowKeyType.T
  type PK = PartitionKey

  def datasetName: String = projection.dataset.toString
  def datasetRef: DatasetRef = projection.dataset

  val rowKeyBinSchema = RecordSchema(rowKeyColumns)
  val binSchema = RecordSchema(dataColumns)

  def dataColumns: Seq[Column] = columns.collect { case d: DataColumn => d }

  /**
   * Returns a new RichProjection with the specified database and everything else kept the same
   */
  def withDatabase(database: String): RichProjection =
    this.copy(projection = this.projection.withDatabase(database))

  def rowKeyFunc: RowReader => RK =
    rowKeyType.getKeyFunc(rowKeyColIndices.toArray)

  val partKeyBinSchema = RecordSchema(partitionColumns)
  val partExtractors = partitionColumns.map(_.extractor).toArray

  // Get the _source_ column index to transform, not the index of computed column definition
  val partIndices = partitionColIndices.map { idx =>
    columns(idx) match {
      case d: DataColumn => idx
      case ComputedColumn(_, _, _, _, Seq(srcIndex), _) => srcIndex
      case other: Column => -1
    }
  }.toArray

  val partitionKeyFunc: RowReader => PartitionKey = { (r: RowReader) =>
    val routedReader = RoutingRowReader(r, partIndices)
    BinaryRecord(partKeyBinSchema, routedReader, partExtractors)
  }

  /**
   * Convenience function to create a BinaryRecord PartitionKey from a Seq of Any.  Handles computed columns.
   */
  def partKey(parts: Any*): PartitionKey =
    BinaryRecord(partKeyBinSchema, SeqRowReader(parts), partExtractors)

  /**
   * Serializes this RichProjection into the minimal string needed to recover a "read-only" RichProjection,
   * ie the minimal RichProjection needed for reads and scans.  This means:
   * - Any ComputedColumns in partition keys are converted to a DataColumn with same type
   * - Any ComputedKeyType is converted to a regular KeyType of the same type
   * - Only readColumns (+ partition and rowkey columns) are left in columns
   * - Most Projection and Dataset info is discarded
   */
  def toReadOnlyProjString(readColumns: Seq[String]): String = {
    val partitionColStrings = partitionColumns.zipWithIndex.map {
      case (ComputedColumn(id, _, _, colType, _, _), i) => DataColumn(id, s"part_$i", "", 0, colType).toString
      case (d: Column, i)                               => d.toString
    }
    val rowkeyColStrings = rowKeyColumns.map(_.toString)
    val extraColStrings = readColumns.map { colName => columns.find(_.name == colName).get.toString }
    Seq(datasetName,
        projection.reverse.toString,
        partitionColStrings.mkString(":"),
        rowkeyColStrings.mkString(":"),
        extraColStrings.mkString(":")).mkString("\u0001")
  }
}

object RichProjection extends StrictLogging {
  import Accumulation._

  sealed trait BadSchema
  case class MissingColumnNames(missing: Seq[String], keyType: String) extends BadSchema
  case class NoColumnsSpecified(keyType: String) extends BadSchema
  case class NoSuchProjectionId(id: Int) extends BadSchema
  case class UnsupportedSegmentColumnType(name: String, colType: Column.ColumnType) extends BadSchema
  case class RowKeyComputedColumns(names: Seq[String]) extends BadSchema
  case class ComputedColumnErrs(errs: Seq[InvalidComputedColumnSpec]) extends BadSchema

  case class BadSchemaError(badSchema: BadSchema) extends Exception(badSchema.toString)

  /**
   * Creates a RichProjection from the dataset and column information, validating errors.
   * @return a RichProjection
   * @throws BadSchemaError
   */
  def apply(dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): RichProjection =
    make(dataset, columns, projectionId).badMap(BadSchemaError).toTry.get

  // Returns computed (generated) columns to add to the schema if needed
  private def getComputedColumns(datasetName: String,
                                 columnNames: Seq[String],
                                 origColumns: Seq[Column]): Seq[Column] Or BadSchema = {
    columnNames.filter(ComputedColumn.isComputedColumn)
               .map { expr => ComputedColumn.analyze(expr, datasetName, origColumns) }
               .combined.badMap { errs => ComputedColumnErrs(errs.toSeq) }
  }

  private def getColumnsFromNames(allColumns: Seq[Column],
                                  columnNames: Seq[String]): Seq[Column] Or BadSchema = {
    if (columnNames.isEmpty) {
      Good(allColumns)
    } else {
      val columnMap = allColumns.map { c => c.name -> c }.toMap
      val missing = columnNames.toSet -- columnMap.keySet
      if (missing.nonEmpty) { Bad(MissingColumnNames(missing.toSeq, "projection")) }
      else                  { Good(columnNames.map(columnMap)) }
    }
  }

  private def getColIndicesAndType(richColumns: Seq[Column],
                                   columnNames: Seq[String],
                                   typ: String): (Seq[Int], Seq[Column], KeyType) Or BadSchema = {
    if (columnNames.isEmpty) {
      Bad(NoColumnsSpecified(typ))
    } else {
      val idToIndex = richColumns.zipWithIndex.map { case (col, i) => col.name -> i }.toMap
      val colIndices = columnNames.map { colName => idToIndex.getOrElse(colName, -1) }
      val notFound = colIndices.zip(columnNames).collect { case (-1, name) => name }
      if (notFound.nonEmpty) return Bad(MissingColumnNames(notFound, typ))
      val columns = colIndices.map(richColumns)
      val keyType = Column.columnsToKeyType(columns)
      val computedColumns = columns.collect { case c: ComputedColumn => c.name }
      if (typ == "row" && computedColumns.nonEmpty) {
        Bad(RowKeyComputedColumns(computedColumns))
      } else {
        Good((colIndices, columns, keyType))
      }
    }
  }

  /**
   * Creates a full RichProjection, validating all the row, partition, and segment keys, and
   * creating proper KeyTypes for each type of key, including handling composite keys and
   * computed functions.
   * @return a Good(RichProjection), or Bad(BadSchema)
   */
  def make(dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): RichProjection Or BadSchema = {
    if (projectionId >= dataset.projections.length) return Bad(NoSuchProjectionId(projectionId))

    val normProjection = dataset.projections(projectionId)

    // NOTE: right now computed columns MUST be at the end, because Filo vectorization can't handle mixing
    val allColIds = dataset.partitionColumns ++ normProjection.keyColIds ++ Seq(normProjection.segmentColId)

    for { computedColumns <- getComputedColumns(dataset.name, allColIds, columns)
          dataColumns <- getColumnsFromNames(columns, normProjection.columns)
          richColumns = dataColumns ++ computedColumns
          // scalac has problems dealing with (a, b, c) <- getColIndicesAndType... apparently
          segStuff <- getColIndicesAndType(richColumns, Seq(normProjection.segmentColId), "segment")
          keyStuff <- getColIndicesAndType(richColumns, normProjection.keyColIds, "row")
          partStuff <- getColIndicesAndType(richColumns, dataset.partitionColumns, "partition") }
    yield {
      val (segColIdx, segCols, segType) = segStuff
      RichProjection(normProjection, dataset, richColumns,
                     segCols.head, segColIdx.head, segType,
                     keyStuff._2, keyStuff._1, keyStuff._3,
                     partStuff._2, partStuff._1)
    }
  }

  /**
   * Recovers a "read-only" RichProjection generated by toReadOnlyProjString().
   */
  def readOnlyFromString(serialized: String): RichProjection = {
    val parts = serialized.split('\u0001')
    val Array(dsName, revStr, partColStr, rowKeyStr) = parts.take(4)
    val partitionColumns = partColStr.split(':').toSeq.map(raw => DataColumn.fromString(raw, dsName))
    val extraColumns = if (parts.size > 4) {
      parts(4).split(':').toSeq.map(raw => DataColumn.fromString(raw, dsName))
    } else {
      Nil
    }
    val rowKeyColumns = rowKeyStr.split(':').toSeq.map(DataColumn.fromString(_, dsName))
    val segmentColumn = ComputedColumn(-1, ":string 0", dsName, Column.ColumnType.StringColumn,
                                       Nil, SingleKeyTypes.StringKeyType.extractor)
    val dsNameParts = dsName.split('.').toSeq
    val ref = dsNameParts match {
      case Seq(db, ds) => DatasetRef(ds, Some(db))
      case Seq(ds)     => DatasetRef(ds)
    }
    val dataset = Dataset(ref, Nil, segmentColumn.name, partitionColumns.map(_.name))

    RichProjection(dataset.projections.head, dataset, extraColumns,
                   segmentColumn, -1, segmentColumn.columnType.keyType,
                   rowKeyColumns, Nil, Column.columnsToKeyType(rowKeyColumns),
                   partitionColumns, Nil)
  }
}