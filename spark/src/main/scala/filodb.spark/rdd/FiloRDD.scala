package filodb.spark.rdd

import java.nio.ByteBuffer

import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{Dataflow, ScanInfo}
import filodb.spark.{Filo, SparkRowReader}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

case class FiloPartition(index: Int, scans: Seq[ScanInfo]) extends Partition

class FiloRDD(@transient val sc: SparkContext,
              splitCount: Int,
              splitSize: Long,
              projection: Projection,
              columns: Seq[ColumnId],
              partition: Option[Any] = None,
              segmentRange: Option[KeyRange[_]] = None
               ) extends RDD[Row](sc, Seq.empty) {

  val filoConfig = Filo.configFromSpark(sc)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    Filo.init(filoConfig)
    val partition = split.asInstanceOf[FiloPartition]
    val scans = partition.scans
    val segmentScans = scans.iterator.flatMap(readSegments(_))
    val sparkRows = segmentScans.map(f => f.map(r => r.asInstanceOf[SparkRowReader]))
    sparkRows.flatten
  }

  private def readSegments(scanInfo: ScanInfo): Iterator[Dataflow] = {
    implicit val rowReaderFactory: Dataflow.RowReaderFactory =
      (chunks: Array[ByteBuffer], classes: Array[Class[_]])
      => new SparkRowReader(chunks: Array[ByteBuffer], classes: Array[Class[_]])

    Filo.parse(Filo.columnStore.readSegments(scanInfo))(f => f.iterator)
  }

  override protected def getPartitions: Array[Partition] = {
    Filo.init(filoConfig)
    val scanSplitsCmd = Filo.columnStore.getScanSplits(splitCount, splitSize, projection,
      columns, partition, segmentRange)
    val scanSplits = Filo.parse(scanSplitsCmd)(s => s)
    scanSplits.zipWithIndex.map { case (group, i) =>
      FiloPartition(i, group)
    }.toArray
  }
}

