package filodb.core.store

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import filodb.core.Types._
import filodb.core._
import filodb.core.metadata.{Column, Projection}
import filodb.core.store.MetaStore.BadSchema

import scala.concurrent.Future
import scala.reflect.ClassTag

object MetaStore {

  case class IllegalColumnChange(reasons: Seq[String]) extends Exception {
    override def getMessage: String = reasons.mkString(", ")
  }

  case class BadSchema(reason: String) extends Exception("BadSchema: " + reason)

}

/**
 * The MetaStore defines an API to read and write dataset/column/projection metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 */
trait MetaStore {

  /**
   * Clears all dataset and column metadata from the MetaStore.
   */
  def clearAllData(): Future[Boolean]

  /**
   * ** Dataset API ***
   */

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param dataset the Dataset to create.  Should have superprojection defined.
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset): Future[Boolean]

  /**
   * Retrieves a Dataset object of the given name
   * @param name Name of the dataset to retrieve
   * @return a Dataset
   */
  def getDataset(name: String): Future[Dataset]

  /**
   * Deletes dataset metadata including all projections.  Does not delete column store data.
   * @param name Name of the dataset to delete.
   * @return Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String): Future[Boolean]

  // TODO: add/delete projections

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param dataset the name of the dataset to return the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  def getSchema(dataset: String, version: Int): Future[Seq[Column]]
}

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned, a partitioning column controls how data is distributed.
 * Data within a dataset consists of multiple projections, including a main "superprojection"
 * that contains all columns.
 */
case class Dataset(name: String,
                   schema: Seq[Column],
                   projectionInfoSeq: Seq[ProjectionInfo],
                   options: DatasetOptions = Dataset.DefaultOptions) {

  val projections = projectionInfoSeq.map {
    _.getProjection(options)
  }
}

/**
 * Config options for a table define operational details for the column store and memtable.
 * Every option must have a default!
 */
case class DatasetOptions(chunkSize: Int,
                          segmentSize: String) {


  override def toString: String = {
    val options = Map[String, Object](
      "chunkSize" -> chunkSize.toString,
      "segmentSize" -> segmentSize
    )
    import scala.collection.JavaConverters._
    val config = ConfigFactory.parseMap(options.asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  def fromString(s: String): DatasetOptions = {
    val config = ConfigFactory.parseString(s)
    DatasetOptions(chunkSize = config.getInt("chunkSize"),
      segmentSize = config.getString("segmentSize"))
  }
}

/**
 * Contains many helper functions especially pertaining to dataset partitioning.
 */
object Dataset {

  val DefaultOptions = DatasetOptions(chunkSize = 1000,
    segmentSize = "10000")

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
        Seq(keyColumn), Seq(sortColumn), Seq(segmentColumn), Seq(partitionColumn)
      )
    )
    Dataset(name, schema, projections)
  }

  /**
   * Returns a SortKeyHelper configured from the DatasetOptions.
   */
  def keyTypeForOptions[K: ClassTag](options: DatasetOptions): SingleKeyType = {
    val StringClass = classOf[String]
    implicitly[ClassTag[K]].runtimeClass match {
      case java.lang.Long.TYPE => new LongKeyType()
      case java.lang.Integer.TYPE => new IntKeyType()
      case java.lang.Double.TYPE => new DoubleKeyType()
      case StringClass => new StringKeyType(options.segmentSize.toInt)
    }
  }

  def keyTypeForOptionsAndColumn[K](options: DatasetOptions, column: Column): Option[SingleKeyType] = {
    import filodb.core.metadata.Column.ColumnType._
    column.columnType match {
      case LongColumn => Some(keyTypeForOptions[Long](options))
      case IntColumn => Some(keyTypeForOptions[Int](options))
      case DoubleColumn => Some(keyTypeForOptions[Double](options))
      case StringColumn => Some(keyTypeForOptions[String](options))
      case other: Column.ColumnType => None
    }
  }

  def keyType(dataset: Dataset, column: Column): Option[SingleKeyType] =
    keyTypeForOptionsAndColumn(dataset.options, column)


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
                          keyColumns: Seq[ColumnId],
                          sortColumns: Seq[ColumnId],
                          segmentColumns: Seq[ColumnId],
                          partitionColumns: Seq[ColumnId],
                          reverse: Boolean = false,
                          includeColumns: Seq[ColumnId] = Nil,
                          // Probably not necessary in the future
                          segmentSize: String = "10000") {

  val schemaMap = schema.map(i => (i.name -> i)).toMap

  val columnIndexes = schema.zipWithIndex.map { case (c, i) => c.name -> i }.toMap


  def getProjection(options: DatasetOptions): Projection = {

    val pType = keyType(options, partitionColumns)
    val kType = keyType(options, keyColumns)
    val oType = keyType(options, sortColumns)
    val sType = keyType(options, segmentColumns)

    Projection(id, dataset, reverse, schema,
      partitionColumns, keyColumns, sortColumns, segmentColumns,
      pType, kType, oType, sType)
  }

  def keyType(options: DatasetOptions, columns: Seq[ColumnId]): KeyType = {
    columns.length match {
      // scalastyle:off
      case no if no <= 0 => throw BadSchema("Invalid number of columns in $keyName")
      case 1 => {
        val col = columns(0)
        Dataset.keyTypeForOptionsAndColumn(options,
          schemaMap.getOrElse(col, throw BadSchema("Invalid column $col")))
          .getOrElse(throw BadSchema("Invalid key column $col"))
      }
      case no if no > 0 => {
        val keyTypes = columns.map(col => schemaMap.getOrElse(col, throw BadSchema("Invalid column $col")))
          .map(Dataset.keyTypeForOptionsAndColumn(options, _).getOrElse(throw BadSchema("Invalid key column $col")))
        CompositeKeyType(keyTypes)
      }
      // scalastyle:on
    }
  }

}
