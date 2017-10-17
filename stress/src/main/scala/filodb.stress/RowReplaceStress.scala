package filodb.stress

import org.apache.spark.sql.{Row, SparkSession, SaveMode}
import scala.util.Random

import filodb.core.Perftools
import filodb.spark._

/**
 * Similar to BatchIngestion, it reads in the NYC Taxi dataset.  However, it is designed to test both
 * ingestion and query speeds when there is a high degree of row replacement.  The idea is to vary
 * the amount of row replacement as well as memtable settings and other things to test things out.
 * It tests that even with rows to be replaced, the resulting row count is accurate as well.
 *
 * To prepare, download the first month's worth of data from http://www.andresmh.com/nyctaxitrips/
 * Also, run this to initialize the filo-stress keyspace:
 *   `filo-cli --database filostress --command init`
 *
 * Arguments:  taxiCsvFilePath [replacementFactor]
 *   - replacementFactor - a ratio of the original number of rows to "replace" or to insert again.
 *                         0 = do not insert any replacement rows
 *                         0.50 = insert 50% more rows which are selected from prev rows
 */
object RowReplaceStress extends App {
  val taxiCsvFile = args(0)
  val replacementFactor = if (args.size > 1) args(1).toDouble else 0.0

  def puts(s: String): Unit = {
    // scalastyle:off
    println(s)
    // scalastyle:on
  }

  val datasetName = sys.props.getOrElse("stress.tablename", "nyc_taxi_replace")
  val keyspaceName = sys.props.getOrElse("stress.keyspace", "filostress")

  // Setup SparkContext, etc.
  val sess = SparkSession.builder.appName("RowReplaceStress")
                                 .config("spark.filodb.cassandra.keyspace", keyspaceName)
                                 .config("spark.sql.shuffle.partitions", "4")
                                 .config("spark.scheduler.mode", "FAIR")
                                 .getOrCreate
  val sc = sess.sparkContext
  import sess.implicits._

  val csvDF = sess.read.format("com.databricks.spark.csv").
                   option("header", "true").option("inferSchema", "true").
                   load(taxiCsvFile)

  // Now, need to transform csvDF and insert replacement rows.
  // Convert to RDD and insert that way.
  // Inject extra chunks by randomly selecting a group of lines from each group of rows
  val groupSize = 25000
  val injectSize = (groupSize * replacementFactor).toInt
  val injectChunkSize = Math.min(groupSize / 8, injectSize)
  val injectReplaceRdd = csvDF.rdd.mapPartitions { rowIt =>
    rowIt.grouped(groupSize).map { rows =>
      val newRows = new collection.mutable.ArrayBuffer[Row]
      newRows ++= rows
      var rowsLeft = injectSize
      while (rowsLeft > 0) {
        val chunkSize = Math.min(injectChunkSize, rowsLeft)
        newRows ++= rows.drop(Random.nextInt(groupSize - chunkSize)).take(chunkSize)
        rowsLeft -= chunkSize
      }
      newRows
    }.flatten
  }
  val injectedDF = sess.createDataFrame(injectReplaceRdd, csvDF.schema)
  val csvLines = csvDF.count()
  val injectedLines = injectedDF.count()

  val ingestMillis = Perftools.timeMillis {
    puts("Starting batch ingestion...")
    injectedDF.sort($"medallion").write.format("filodb.spark").
      option("dataset", datasetName).
      option("row_keys", "pickup_datetime,hack_license,medallion,pickup_longitude").
      option("partition_columns", ":monthOfYear pickup_datetime,:stringPrefix medallion 2").
      mode(SaveMode.Overwrite).save()
    puts("Batch ingestion done.")
  }

  val df = sess.filoDataset(datasetName)
  df.createOrReplaceTempView(datasetName)

  puts(s"Waiting a few seconds for ingestion to finish...")
  Thread sleep 5000

  val readMillis = Perftools.timeMillis { sess.sql(s"select avg(passenger_count) from $datasetName").show }

  val count = df.count()
  puts(s"\n\n---------------------\n\n")
  if (count == csvLines) { puts(s"Count matched $count for dataframe $df") }
  else                   { puts(s"Expected $csvLines rows, but actually got $count for dataframe $df") }

  puts(s"Original CSV file of $csvLines lines expanded to $injectedLines lines for testing replacemnent")
  puts(s"Ingestion took $ingestMillis ms, reading took $readMillis ms")

  // clean up!
  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sc.stop()
}