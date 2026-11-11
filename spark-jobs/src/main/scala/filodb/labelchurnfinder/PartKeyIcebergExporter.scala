package filodb.labelchurnfinder

import java.sql.Date
import java.time.{Instant, ZoneOffset}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Writes a daily snapshot of the partKeys of the configured workspaces to one Iceberg table
 * <catalog>.<database>.<table>, partitioned by (snapshot_date, mosaic_partition).
 * A run replaces only its own (snapshot_date, mosaic_partition) partition.
 *
 * Requires following typesafe config properties:
 *
 * {{{
 * filodb.labelchurnfinder.iceberg-export {
 *   catalog = "mosaic_catalog"
 *   database = "part_keys_perf"
 *   table = "daily_snapshot"
 *   base-path = "s3a://bucket/data-export-iceberg/mosaic_catalog"
 *   workspaces = ["ase-trusted-cloud"]
 * }
 * }}}
 */
class PartKeyIcebergExporter(filodbConfig: Config) extends StrictLogging {
  private val exportConfigPath = "labelchurnfinder.iceberg-export"
  private val catalog = filodbConfig.as[String](s"$exportConfigPath.catalog")
  private val database = filodbConfig.as[String](s"$exportConfigPath.database")
  private val table = filodbConfig.as[String](s"$exportConfigPath.table")
  private val basePath = filodbConfig.as[String](s"$exportConfigPath.base-path")
  private val workspacePatterns = filodbConfig.as[Seq[String]](s"$exportConfigPath.workspaces").map(_.r.pattern)
  private val mosaicPartition = filodbConfig.as[String]("deployment-partition-name")

  /**
   * Maps the time series to the rows of the Iceberg table.
   */
  private[labelchurnfinder] def exportRows(partKeys: Dataset[PartKeyRow], snapshotTs: Long): DataFrame = {
    val snapshotDate = Date.valueOf(Instant.ofEpochMilli(snapshotTs).atZone(ZoneOffset.UTC).toLocalDate)
    partKeys.select(
      col("ws"),
      col("ns"),
      col("metric"),
      col("labels"),
      col("startTime").alias("start_time"),
      col("endTime").alias("end_time"),
      lit(mosaicPartition).alias("mosaic_partition"),
      lit(snapshotDate).alias("snapshot_date"),
      lit(snapshotTs).alias("snapshot_ts"))
  }

  /**
   * Writes the time series of the configured workspaces to the Iceberg table.
   */
  def exportPartKeys(spark: SparkSession, partKeys: Dataset[PartKeyRow]): Unit = {
    require(basePath.nonEmpty, s"filodb.$exportConfigPath.base-path must be set when the export is enabled")
    import partKeys.sparkSession.implicits._
    val workspaces = partKeys.select("ws").as[String].distinct().collect()
      .filter(ws => workspacePatterns.exists(_.matcher(ws).matches))
    logger.info(s"Exporting partKeys to Iceberg for ${workspaces.length} workspaces, " +
      s"mosaicPartition=$mosaicPartition")
    if (workspaces.nonEmpty) {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $catalog.$database")
      spark.sql(sqlCreateTable)
      exportRows(partKeys, System.currentTimeMillis())
        .filter(col("ws").isin(workspaces: _*))
        .sortWithinPartitions("ws")
        .writeTo(s"$catalog.$database.$table")
        .option("distribution-mode", "none")
        .overwritePartitions()
    }
  }

  private def sqlCreateTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $catalog.$database.`$table` (
       | ws string,
       | ns string,
       | metric string,
       | labels map<string, string>,
       | start_time long,
       | end_time long,
       | mosaic_partition string,
       | snapshot_date date,
       | snapshot_ts long
       | )
       | USING iceberg
       | PARTITIONED BY (snapshot_date, mosaic_partition)
       | LOCATION '$basePath/$database/$table'
       |""".stripMargin
}
