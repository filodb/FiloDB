package filodb.spark

import akka.actor.{Actor, ActorRef, Props}
import java.util.concurrent.BlockingQueue
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.velvia.filo.RowReader

import filodb.core.{DatasetRef, Types}
import filodb.coordinator.{BaseActor, IngestionCommands, RowSource}

object RddRowSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 10
  val DefaultRowsToRead = 1000

  def props(queue: BlockingQueue[Seq[Row]],
            columns: Seq[String],
            dataset: DatasetRef,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches): Props =
    Props(classOf[RddRowSourceActor], queue, columns, dataset, version,
          coordinatorActor, maxUnackedBatches)
}

/**
 * A source actor for ingesting from one partition of a Spark DataFrame/SchemaRDD.
 * Assumes that the columns in the DataFrame have already been verified to be supported
 * by FiloDB and exist in the dataset already.
 *
 * TODO: implement the rewind() function, store unacked rows so we can replay them
 */
class RddRowSourceActor(queue: BlockingQueue[Seq[Row]],
                        columns: Seq[String],
                        val dataset: DatasetRef,
                        val version: Int,
                        val coordinatorActor: ActorRef,
                        val maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches)
extends BaseActor with RowSource {
  import IngestionCommands._
  import RddRowSourceActor._

  def getStartMessage(): SetupIngestion = SetupIngestion(dataset, columns, version)

  val seqIds = Iterator.from(0).map { id => id.toLong }

  val groupedRows = new Iterator[Seq[RowReader]] {
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
  val batchIterator = seqIds.zip(groupedRows)
}

case class RddRowReader(row: Row) extends RowReader {
  def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
  def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
  def getInt(columnNo: Int): Int = row.getInt(columnNo)
  def getLong(columnNo: Int): Long = row.getLong(columnNo)
  def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
  def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
  def getString(columnNo: Int): String = row.getString(columnNo)
  def getAny(columnNo: Int): Any = row.get(columnNo)
}
