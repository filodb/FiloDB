package filodb.labelchurnfinder

import scala.collection.mutable

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon
import monix.execution.Scheduler
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.downsampler.index.DSIndexJobSettings

object LabelChurnFinder extends App {
  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)
  val labelChurnFinder = new LabelChurnFinder(dsSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  val result = labelChurnFinder.run(sparkConf)
  val sortedResult = result.toArray.sortBy(-_._2.churn()) // negative for descending order
  sortedResult.foreach { case (key, sketch) =>
    // TODO, for now logging to log files. Can write this to durable store in subsequent iterations
    val labelActiveCard = Math.round(sketch.active.getEstimate)
    val labelTotalCard = Math.round(sketch.total.getEstimate)
    LCFContext.log.info(s"Estimated label cardinality: label=$key " +
      s"active=$labelActiveCard total=$labelTotalCard churn=${labelTotalCard / labelActiveCard}")
  }
}

object LCFContext extends StrictLogging {
  lazy protected[labelchurnfinder] val log: Logger = logger
  lazy val sched = Scheduler.io("cass-read-sched")
}

case class ChurnSketches(active: CpcSketch, total: CpcSketch) {
  def churn(): Double = {
    val activeCard = active.getEstimate
    val totalCard = total.getEstimate
    if (activeCard == 0) 0.0 else totalCard / activeCard
  }
}

/**
 * Requires following typesafe config properties:
 *
 *
 * filodb.labelchurnfinder.pk-filters = [
 *  {
 *     _ns_ = "bulk_ns"
 *     _ws_ = "tag_value_as_regex"
 *  }
 * ]
 *
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings) extends Serializable with StrictLogging {

  // scalastyle:off
  def run(conf: SparkConf): mutable.HashMap[Seq[String], ChurnSketches] = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CpcSketch]));

    val spark = SparkSession.builder()
      .appName("LabelChurnFinder")
      .config(conf)
      .getOrCreate()


    LCFContext.log.info(s"This is the Label Churn Finder. Starting job.")
    val lcfTask = new LcfTask(dsSettings)

    val splits = if (lcfTask.colStore.partKeysV2TableEnabled) {
      spark.sparkContext
        .makeRDD(lcfTask.colStore.getScanSplits(lcfTask.rawDatasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
        }.map( sp => (sp, -1))
    } else {
      spark.sparkContext
        .makeRDD(lcfTask.colStore.getScanSplits(lcfTask.rawDatasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
            .flatMap { split =>
              for { shard <- 0 until lcfTask.numShards } yield (split, shard)
            }
        }
    }

    val result = splits.map { case (split, shard) =>
      lcfTask.computeLabelChurn(split, shard)
    }.fold(mutable.HashMap.empty[Seq[String], ChurnSketches]) { (acc, m) =>
      m.foreach { case (key, sketch) =>
        if (acc.contains(key)) {
          val unionActive = new CpcUnion(lcfTask.logK)
          unionActive.update(acc(key).active)
          unionActive.update(sketch.active)
          val unionTotal = new CpcUnion(lcfTask.logK)
          unionTotal.update(acc(key).total)
          unionTotal.update(sketch.total)
          acc.put(key, ChurnSketches(unionActive.getResult, unionTotal.getResult))
        } else {
          acc.put(key, sketch)
        }
      }
      acc
    }
    LCFContext.log.info(s"LabelChurnFinder completed successfully")
    spark.close()
    result
  }

}
