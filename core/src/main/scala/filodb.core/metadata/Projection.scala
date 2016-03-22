package filodb.core.metadata

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalactic._
import org.velvia.filo.RowReader

import filodb.core.{CompositeKeyType, KeyType, KeyRange, BinaryKeyRange}
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
    s"  Segment column: $segmentColId\n" +
    s"  Projection columns: ${columns.mkString(", ")}"

  /**
   * Returns a new Projection with the specified database and everything else kept the same
   */
  def withDatabase(database: String): Projection =
    this.copy(dataset = this.dataset.copy(database = Some(database)))
}

/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 * It is also guaranteed to have a valid segmentColumn and dataset partition columns.
 */
case class RichProjection(projection: Projection,
                          dataset: Dataset,
                          columns: Seq[Column],
                          segmentColumn: Column,
                          segmentColIndex: Int,
                          segmentType: KeyType,
                          rowKeyColumns: Seq[Column],
                          rowKeyColIndices: Seq[Int],
                          rowKeyType: KeyType,
                          partitionColumns: Seq[Column],
                          partitionColIndices: Seq[Int],
                          partitionType: KeyType) {
  type SK = segmentType.T
  type RK = rowKeyType.T
  type PK = partitionType.T

  def datasetName: String = projection.dataset.toString
  def datasetRef: DatasetRef = projection.dataset

  /**
   * Returns a new RichProjection with the specified database and everything else kept the same
   */
  def withDatabase(database: String): RichProjection =
    this.copy(projection = this.projection.withDatabase(database))

  def segmentKeyFunc: RowReader => SK =
    segmentType.getKeyFunc(Array(segmentColIndex))

  def rowKeyFunc: RowReader => RK =
    rowKeyType.getKeyFunc(rowKeyColIndices.toArray)

  def partitionKeyFunc: RowReader => PK =
    partitionType.getKeyFunc(partitionColIndices.toArray)

  def toBinaryKeyRange[PK, SK](keyRange: KeyRange[PK, SK]): BinaryKeyRange =
    BinaryKeyRange(partitionType.toBytes(keyRange.partition.asInstanceOf[partitionType.T]),
                   segmentType.toBytes(keyRange.start.asInstanceOf[segmentType.T]),
                   segmentType.toBytes(keyRange.end.asInstanceOf[segmentType.T]),
                   keyRange.endExclusive)

  def toBinarySegRange[SK](segmentRange: SegmentRange[SK]): BinarySegmentRange =
    BinarySegmentRange(segmentType.toBytes(segmentRange.start.asInstanceOf[segmentType.T]),
                       segmentType.toBytes(segmentRange.end.asInstanceOf[segmentType.T]))

  /**
   * Serializes this RichProjection into the minimal string needed to recover a "read-only" RichProjection,
   * ie the minimal RichProjection needed for segment reads and scans.  This means:
   * - Row key columns are not preserved
   * - Any ComputedColumns in partition / segment keys are converted to a DataColumn with same type
   * - Any ComputedKeyType is converted to a regular KeyType of the same type
   * - Only readColumns (+ partition and segment columns) are left in columns
   * - Most Projection and Dataset info is discarded
   */
  def toReadOnlyProjString(readColumns: Seq[String]): String = {
    val partitionColStrings = partitionColumns.zipWithIndex.map {
      case (ComputedColumn(id, _, _, colType, _, _), i) => DataColumn(id, s"part_$i", "", 0, colType).toString
      case (d: Column, i)                               => d.toString
    }
    val segmentColString = segmentColumn match {
      case ComputedColumn(id, _, _, colType, _, _) => DataColumn(id, "segCol", "", 0, colType).toString
      case d: Column                               => d.toString
    }
    val extraColStrings = readColumns.map { colName => columns.find(_.name == colName).get.toString }
    Seq(datasetName,
        projection.reverse.toString,
        partitionColStrings.mkString(":"),
        segmentColString,
        extraColStrings.mkString(":")).mkString("\001")
  }

  /**
   * Creates a new RichProjection intended only for ChunkMergingStrategy.mergeSegments...
   * it reads only the source columns needed to recreate the row key, so the row key functions need
   * to be recomputed.
   */
  def toRowKeyOnlyProjection: RichProjection = {
    // First, reduce set of columns to row key columns (including any computed source columns)
    val newColumns: Seq[Column] = rowKeyColumns.flatMap {
      case c: ComputedColumn => c.sourceColumns.map { srcCol => columns.find(_.name == srcCol).get }
      case d: Column => Seq(d)
    }
    // Recompute any computed columns based on new column indices
    val rkColumnsIndices = rowKeyColumns.map {
      case c: ComputedColumn => (ComputedColumn.analyze(c.expr, datasetName, newColumns).get, -1)
      case d: Column         => (d, newColumns.indexWhere(_.name == d.name))
    }
    val newRkColumns = rkColumnsIndices.map(_._1)
    val newRkIndices = rkColumnsIndices.map(_._2)
    val newRkType = Column.columnsToKeyType(newRkColumns)
    this.copy(columns = newColumns,
              rowKeyColumns = newRkColumns,
              rowKeyColIndices = newRkIndices,
              rowKeyType = newRkType)
  }
}

object RichProjection extends StrictLogging {
  import Accumulation._

  sealed trait BadSchema
  case class MissingColumnNames(missing: Seq[String], keyType: String) extends BadSchema
  case class NoColumnsSpecified(keyType: String) extends BadSchema
  case class NoSuchProjectionId(id: Int) extends BadSchema
  case class UnsupportedSegmentColumnType(name: String, colType: Column.ColumnType) extends BadSchema
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
      if (typ == "segment" && !keyType.isSegmentType) {
        Bad(UnsupportedSegmentColumnType(columnNames.head, columns.head.columnType))
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
                     partStuff._2, partStuff._1, partStuff._3)
    }
  }

  /**
   * Recovers a "read-only" RichProjection generated by toReadOnlyProjString().
   */
  def readOnlyFromString(serialized: String): RichProjection = {
    val parts = serialized.split('\001')
    val Array(dsName, revStr, partColStr, segStr) = parts.take(4)
    val partitionColumns = partColStr.split(':').toSeq.map(raw => DataColumn.fromString(raw, dsName))
    val extraColumns = if (parts.size > 4) {
      parts(4).split(':').toSeq.map(raw => DataColumn.fromString(raw, dsName))
    } else {
      Nil
    }
    val segmentColumn = DataColumn.fromString(segStr, dsName)
    val dsNameParts = dsName.split('.').toSeq
    val ref = dsNameParts match {
      case Seq(db, ds) => DatasetRef(ds, Some(db))
      case Seq(ds)     => DatasetRef(ds)
    }
    val dataset = Dataset(ref, Nil, segmentColumn.name, partitionColumns.map(_.name))

    RichProjection(dataset.projections.head, dataset, extraColumns,
                   segmentColumn, -1, Column.columnsToKeyType(Seq(segmentColumn)),
                   Nil, Nil, CompositeKeyType(Nil),
                   partitionColumns, Nil, Column.columnsToKeyType(partitionColumns))
  }
}