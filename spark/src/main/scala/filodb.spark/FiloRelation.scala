package filodb.spark

import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core.metadata.{Column, KeyRange}
import filodb.core.store.Dataset
import filodb.spark.rdd.FiloRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.velvia.filo.ArrayStringRowReader

import scala.language.postfixOps

object FiloRelation {

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global


  def getDatasetObj(dataset: String): Dataset =
    Filo.parse(Filo.metaStore.getDataset(dataset)) { ds => ds.get }

  def getSchema(dataset: String, version: Int): Seq[Column] =
    Filo.parse(Filo.metaStore.getSchema(dataset)) { schema => schema }

}

/**
 * Schema and row scanner, with pruned column optimization for fast reading from FiloDB
 * NOTE: Each Spark partition is given 1 to N Filo partitions, and the code sequentially
 * reads data from each partition
 *
 */
case class FiloRelation(dataset: String,
                        version: Int = 0,
                        splitsPerNode: Int = 1)
                       (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedFilteredScan with StrictLogging {

  import filodb.spark.FiloRelation._
  import filodb.spark.TypeConverters._

  val sc = sqlContext.sparkContext
  val filoConfig = Filo.init(sc)

  val datasetObj = getDatasetObj(dataset)
  val filoSchema = getSchema(dataset, version)
  val superProjection = datasetObj.superProjection
  val partitionColumns = datasetObj.partitionColumns

  val schema = StructType(columnsToSqlFields(filoSchema))
  logger.info(s"Read schema for dataset $dataset = $schema")

  def buildScan(): RDD[Row] = buildScan(superProjection.columnNames.toArray, Array.empty[Filter])

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val partitionOption = getPartitionKey(filters)

    val suitableProjections = datasetObj.projections
      .filter(p => requiredColumns forall (p.columnNames contains))

    val bestAvailable = suitableProjections
      .find(p => getSegmentFilters(p.segmentColumns, filters).nonEmpty)

    bestAvailable match {
      //there is a segment range clause in the match
      case Some(projection) =>
        val segmentRangeOption = getSegmentRange(projection.segmentColumns, filters)
        new FiloRDD(sc, splitsPerNode, Long.MaxValue, projection, requiredColumns, partitionOption, segmentRangeOption)

      case None =>
        val projection = suitableProjections.headOption.getOrElse(datasetObj.superProjection)
        new FiloRDD(sc, splitsPerNode, Long.MaxValue, projection, requiredColumns, partitionOption)
    }
  }


  private def getPartitionKey(filters: Array[Filter]): Option[Any] = {
    val partitionFilters = getPartitionFilters(filters)
    val containsPartitionKey = partitionFilters.length == partitionColumns.length
    if (containsPartitionKey) {
      val valueMap = partitionFilters.map { case EqualTo(col, value) => col -> value }.toMap
      val rowReader = new ArrayStringRowReader(
        filoSchema.map(col => valueMap.getOrElse(col.name, "").toString).toArray
      )
      Some(superProjection.partitionFunction(rowReader))
    } else None
  }

  private def getSegmentRange(segmentColumns: Seq[String],
                              filters: Array[Filter]): Option[KeyRange[_]] = {
    val segmentFilters = getSegmentFilters(segmentColumns, filters)
    segmentFilters.length match {

      case 1 =>
        val predicate = segmentFilters.head
        predicate match {
          case EqualTo(columnName, value) =>
            Some(KeyRange(Some(value), Some(value)))
          case LessThan(columnName, value) =>
            Some(KeyRange(None, Some(value), startExclusive = false, endExclusive = true))
          case LessThanOrEqual(columnName, value) =>
            Some(KeyRange(None, Some(value), startExclusive = false, endExclusive = false))
          case GreaterThan(columnName, value) =>
            Some(KeyRange(Some(value), None, startExclusive = true, endExclusive = false))
          case GreaterThanOrEqual(columnName, value) =>
            Some(KeyRange(Some(value), None, startExclusive = false, endExclusive = false))
          case _ => None
        }
      case 2 =>
        //ordered by class name in reverse i.e Lt comes first, then Lte, then Gt then Gte
        val orderedFilters = segmentFilters.sortBy(_.getClass.getName).reverse
        val predicate = (orderedFilters.head, orderedFilters.last)

        predicate match {
          case (LessThan(_, start), GreaterThan(_, end)) =>
            Some(KeyRange(Some(start), Some(end), startExclusive = true, endExclusive = true))
          case (LessThanOrEqual(_, start), GreaterThan(_, end)) =>
            Some(KeyRange(Some(start), Some(end), startExclusive = false, endExclusive = true))
          case (LessThanOrEqual(_, start), GreaterThanOrEqual(_, end)) =>
            Some(KeyRange(Some(start), Some(end), startExclusive = false, endExclusive = false))
          case (LessThan(_, start), GreaterThanOrEqual(_, end)) =>
            Some(KeyRange(Some(start), Some(end), startExclusive = true, endExclusive = false))
          case _ => None
        }

      case _ => None

    }

  }

  private def getPartitionFilters(filters: Array[Filter]) = filters.filter {
    case predicate =>
      predicate match {
        case EqualTo(col, _) => partitionColumns.contains(col)
        case _ => false
      }
  }

  private def getSegmentFilters(segmentColumns: Seq[String],
                                filters: Array[Filter]) = filters.filter {
    case predicate =>
      predicate match {
        case EqualTo(columnName, _) if segmentColumns.contains(columnName) => true
        case LessThan(columnName, _) if segmentColumns.contains(columnName) => true
        case LessThanOrEqual(columnName, _) if segmentColumns.contains(columnName) => true
        case GreaterThan(columnName, _) if segmentColumns.contains(columnName) => true
        case GreaterThanOrEqual(columnName, _) if segmentColumns.contains(columnName) => true
        case In(columnName, _) if segmentColumns.contains(columnName) => true
        case _ => false
      }
  }


}

