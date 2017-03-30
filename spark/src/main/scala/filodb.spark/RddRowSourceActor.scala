package filodb.spark

import akka.actor.{Actor, ActorRef, Props}
import java.sql.Timestamp
import java.util.concurrent.BlockingQueue
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.joda.time.DateTime
import org.velvia.filo.RowReader

import filodb.coordinator.{BaseActor, RowSource}
import filodb.core.metadata.RichProjection
import filodb.core.{DatasetRef, Types}


object RddRowSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 10
  val DefaultRowsToRead = 1000

  def props(queue: BlockingQueue[Seq[Row]],
            projection: RichProjection,
            version: Int,
            clusterActor: ActorRef,
            maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches): Props =
    Props(classOf[RddRowSourceActor], queue, projection, version,
          clusterActor, maxUnackedBatches)
}

/**
 * A source actor for ingesting from one partition of a Spark DataFrame/SchemaRDD.
 * Assumes that the columns in the DataFrame have already been verified to be supported
 * by FiloDB and exist in the dataset already.
 */
class RddRowSourceActor(queue: BlockingQueue[Seq[Row]],
                        val projection: RichProjection,
                        val version: Int,
                        val clusterActor: ActorRef,
                        val maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches)
extends BaseActor with RowSource {
  import RddRowSourceActor._

  val batchIterator = new Iterator[Seq[RowReader]] {
    var done = false
    var current: Seq[Row] = Seq.empty
    def hasNext: Boolean = {
      if (!done && current.isEmpty) {
        current = queue.take()
        if (current.isEmpty) done = true
      }
      !done
    }
    def next: Seq[RowReader] = {
      val readers = current.map(RddRowReader)
      // Reset current so we don't keep pulling in hasNext
      current = Seq.empty
      readers
    }
  }
}

case class RddRowReader(row: Row) extends RowReader {
  def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
  def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
  def getInt(columnNo: Int): Int = row.getInt(columnNo)
  def getLong(columnNo: Int): Long =
    try { row.getLong(columnNo) }
    catch {
      case e: ClassCastException => row.getAs[Timestamp](columnNo).getTime
    }
  def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
  def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
  def getString(columnNo: Int): String = row.getString(columnNo)
  def getAny(columnNo: Int): Any = row.get(columnNo)
}
