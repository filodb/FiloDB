package filodb.spark.rdd

import java.nio.ByteBuffer

import com.typesafe.config.Config
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{Dataflow, ScanInfo, ScanSplit}
import filodb.spark.{Filo, SparkRowReader}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

case class FiloPartition(index: Int, scanSplit: ScanSplit) extends Partition {
  val replicas = scanSplit.replicas
  val scans = scanSplit.scans
}

class FiloRDD(@transient val sc: SparkContext,
              filoConfig: Config,
              splitCount: Int,
              splitSize: Long,
              projection: Projection,
              columns: Seq[ColumnId],
              partition: Option[Any] = None,
              segmentRange: Option[KeyRange[_]] = None
               ) extends RDD[Row](sc, Seq.empty) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    Filo.init(filoConfig)
    val partition = split.asInstanceOf[FiloPartition]
    val scans = partition.scans
    val segmentScans = scans.iterator.flatMap(readSegments)
    val sparkRows = segmentScans.map(f => f.map(r => r.asInstanceOf[SparkRowReader]))
    sparkRows.flatten
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[FiloPartition].replicas

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

