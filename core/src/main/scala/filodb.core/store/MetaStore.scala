package filodb.core.store

import filodb.core.Types._
import filodb.core._
import filodb.core.metadata.{Column, Projection}
import filodb.core.store.MetaStore.BadSchema

import scala.concurrent.Future
import scala.reflect.ClassTag

object MetaStore {

  case class BadSchema(reason: String) extends Exception("BadSchema: " + reason)

}

/**
 * The MetaStore defines an API to read and write dataset/column/projection metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 */
trait MetaStore {

  /**
   * Retrieves a Dataset object of the given name
   * @param name Name of the dataset to retrieve
   * @return a Dataset
   */
  def getDataset(name: String): Future[Option[Dataset]]

  def getProjection(name: String, projectionId: Int): Future[ProjectionInfo]

  def addProjection(projectionInfo: ProjectionInfo): Future[Boolean]


  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param dataset the name of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  def getSchema(dataset: String): Future[Seq[Column]]
}

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned, a partitioning column controls how data is distributed.
 * Data within a dataset consists of multiple projections, including a main "superprojection"
 * that contains all columns.
 */
case class Dataset(name: String,
                   schema: Seq[Column],
                   projectionInfoSeq: Seq[ProjectionInfo]) {

  val projections = projectionInfoSeq.map(_.getProjection)

  val superProjection = projections.head

  val partitionColumns = superProjection.partitionColumns

}

/**
 * Contains many helper functions especially pertaining to dataset partitioning.
 */
object Dataset {

  /**
   * Creates a new Dataset with a single superprojection with a defined sort order.
   */
  def apply(name: String,
            schema: Seq[Column],
            partitionColumn: ColumnId,
            keyColumn: ColumnId,
            sortColumn: ColumnId,
            segmentColumn: ColumnId
             ): Dataset = {
    val projections = Seq(
      ProjectionInfo(0, name, schema,
        Seq(partitionColumn), Seq(keyColumn), Seq(sortColumn), Seq(segmentColumn)
      )
    )
    Dataset(name, schema, projections)
  }

  /**
   * Returns a SortKeyHelper configured from the DatasetOptions.
   */
  def keyType[K: ClassTag](): SingleKeyType = {
    val StringClass = classOf[String]
    implicitly[ClassTag[K]].runtimeClass match {
      case java.lang.Long.TYPE => new LongKeyType()
      case java.lang.Integer.TYPE => new IntKeyType()
      case java.lang.Double.TYPE => new DoubleKeyType()
      case StringClass => new StringKeyType()
    }
  }

  def keyTypeForColumn[K](column: Column): Option[SingleKeyType] = {
    import filodb.core.metadata.Column.ColumnType._
    column.columnType match {
      case LongColumn => Some(keyType[Long]())
      case IntColumn => Some(keyType[Int]())
      case DoubleColumn => Some(keyType[Double]())
      case StringColumn => Some(keyType[String]())
      case other: Column.ColumnType => None
    }
  }

  def keyType(dataset: Dataset, column: Column): Option[SingleKeyType] =
    keyTypeForColumn(column)


}

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 *
 * The Projection base class is normalized, ie it doesn't have all the information.
 */
case class ProjectionInfo(id: Int,
                          dataset: TableName,
                          schema: Seq[Column],
                          partitionColumns: Seq[ColumnId],
                          keyColumns: Seq[ColumnId],
                          sortColumns: Seq[ColumnId],
                          segmentColumns: Seq[ColumnId],
                          reverse: Boolean = false,
                          includeColumns: Seq[ColumnId] = Nil,
                          // Probably not necessary in the future
                          segmentSize: String = "10000") {

  val schemaMap = schema.map(i => i.name -> i).toMap

  val columnIndexes = schema.zipWithIndex.map { case (c, i) => c.name -> i }.toMap


  def getProjection: Projection = {

    val pType = keyType(partitionColumns)
    val kType = keyType(keyColumns)
    val oType = keyType(sortColumns)
    val sType = keyType(segmentColumns)

    Projection(id, dataset, reverse, schema,
      partitionColumns, keyColumns, sortColumns, segmentColumns,
      pType, kType, oType, sType)
  }

  def keyType(columns: Seq[ColumnId]): KeyType = columns.length match {
    // scalastyle:off
    case no if no <= 0 => throw BadSchema("Invalid number of columns in $keyName")
    case 1 =>
      val col = columns.head
      Dataset.keyTypeForColumn(
        schemaMap.getOrElse(col, throw BadSchema("Invalid column $col")))
        .getOrElse(throw BadSchema("Invalid key column $col"))
    case no if no > 0 =>
      val keyTypes = columns.map(col => schemaMap.getOrElse(col, throw BadSchema("Invalid column $col")))
        .map(Dataset.keyTypeForColumn(_).getOrElse(throw BadSchema("Invalid key column $col")))
      CompositeKeyType(keyTypes)
    // scalastyle:on
  }

}
