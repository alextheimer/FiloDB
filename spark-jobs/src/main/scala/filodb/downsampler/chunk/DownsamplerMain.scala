package filodb.downsampler.chunk

import java.time.Instant
import java.time.format.DateTimeFormatter
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import filodb.coordinator.KamonShutdownHook
import filodb.downsampler.DownsamplerContext


import java.util.concurrent.Executors

/**
 * Implement this trait and provide its fully-qualified name as the downsampler config:
 *     spark-session-factory = "org.fully.qualified.FactoryClass"
 */
trait SparkSessionFactory {
  def make(sparkConf: SparkConf): SparkSession
}

class DefaultSparkSessionFactory extends SparkSessionFactory {
  override def make(sparkConf: SparkConf): SparkSession = {
    SparkSession.builder()
      .appName("FiloDBDownsampler")
      .config(sparkConf)
      .getOrCreate()
  }
}

/**
  *
  * Goal: Downsample all real-time data.
  * Goal: Align chunks when this job is run in multiple DCs so that cross-dc repairs can be done.
  * Non-Goal: Downsampling of non-real time data or data with different epoch.
  *
  * Strategy is to run this spark job every 6 hours at 8am, 2pm, 8pm, 2am UTC each day.
  *
  * Run at 8am: We query data with ingestionTime from 10pm to 8am.
  *             Then query and downsample data with userTime between 12am to 6am.
  *             Downsampled chunk would have an ingestionTime of 12am.
  * Run at 2pm: We query data with ingestionTime from 4am to 2pm.
  *             Then query and downsample data with userTime between 6am to 12pm.
  *             Downsampled chunk would have an ingestionTime of 6am.
  * Run at 8pm: We query data with ingestionTime from 10am to 8pm.
  *             Then query and downsample data with userTime between 12pm to 6pm.
  *             Downsampled chunk would have an ingestionTime of 12pm.
  * Run at 2am: We query data with ingestionTime from 4pm to 2am.
  *             Then query and downsample data with userTime between 6pm to 12am.
  *             Downsampled chunk would have an ingestionTime of 6pm.
  *
  * This will cover all data with userTime 12am to 12am.
  * Since we query for a broader ingestionTime, it will include data arriving early/late by 2 hours.
  *
  * Important Note: The reason non-real-time data is not included in goals is because we
  * want chunk alignment between DCs in downsampled data to enable cross-dc repair without chunk surgery.
  * Without chunk-alignment in raw data and consistency in behavior across DCs, it would be difficult
  * to achieve chunk alignment in downsampled data. Once we solve that (deferred problem), we will
  * lift the constraint.
  */
object DownsamplerMain extends App {

  Kamon.init()  // kamon init should be first thing in driver jvm
  val settings = new DownsamplerSettings()
  val d = new Downsampler(settings)
  val sparkConf = new SparkConf(loadDefaults = true)
  d.run(sparkConf)
}

class Downsampler(settings: DownsamplerSettings) extends Serializable {

  lazy val exportLatency =
    Kamon.histogram("export-latency", MeasurementUnit.time.milliseconds).withoutTags()

  // Gotcha!! Need separate function (Cannot be within body of a class)
  // to create a closure for spark to serialize and move to executors.
  // Otherwise, config values below were not being sent over.
  // See https://medium.com/onzo-tech/serialization-challenges-with-spark-and-scala-a2287cd51c54
  // scalastyle:off method.length
  def run(sparkConf: SparkConf): SparkSession = {

    val spark = Class.forName(settings.sparkSessionFactoryClass)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[SparkSessionFactory]
        .make(sparkConf)

    DownsamplerContext.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")

    // Use the spark property spark.filodb.downsampler.user-time-override to override the
    // userTime period for which downsampling should occur.
    // Generally disabled, defaults the period that just ended prior to now.
    // Specified during reruns for downsampling old data
    val userTimeInPeriod: Long = spark.sparkContext.getConf
      .getOption("spark.filodb.downsampler.userTimeOverride") match {
        // by default assume a time in the previous downsample period
        case None => System.currentTimeMillis() - settings.downsampleChunkDuration
        // examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
        case Some(str) => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli()
      }

    val userTimeStart: Long = (userTimeInPeriod / settings.downsampleChunkDuration) * settings.downsampleChunkDuration
    val userTimeEndExclusive: Long = userTimeStart + settings.downsampleChunkDuration
    val ingestionTimeStart: Long = userTimeStart - settings.widenIngestionTimeRangeBy.toMillis
    val ingestionTimeEnd: Long = userTimeEndExclusive + settings.widenIngestionTimeRangeBy.toMillis

    val downsamplePeriodStr = java.time.Instant.ofEpochMilli(userTimeStart).toString

    val batchDownsampler = new BatchDownsampler(settings, userTimeStart, userTimeEndExclusive)
    val batchExporter = new BatchExporter(settings, userTimeStart, userTimeEndExclusive)

    DownsamplerContext.dsLogger.info(s"This is the Downsampling driver. Starting downsampling job " +
      s"rawDataset=${settings.rawDatasetName} for " +
      s"userTimeInPeriod=${java.time.Instant.ofEpochMilli(userTimeInPeriod)} " +
      s"ingestionTimeStart=${java.time.Instant.ofEpochMilli(ingestionTimeStart)} " +
      s"ingestionTimeEnd=${java.time.Instant.ofEpochMilli(ingestionTimeEnd)} " +
      s"userTimeStart=$downsamplePeriodStr " +
      s"userTimeEndExclusive=${java.time.Instant.ofEpochMilli(userTimeEndExclusive)}")
    DownsamplerContext.dsLogger.info(s"To rerun this job add the following spark config: " +
      s""""spark.filodb.downsampler.userTimeOverride": "${java.time.Instant.ofEpochMilli(userTimeInPeriod)}"""")

    val splits = batchDownsampler.rawCassandraColStore.getScanSplits(batchDownsampler.rawDatasetRef)
    DownsamplerContext.dsLogger.info(s"Cassandra split size: ${splits.size}. We will have this many spark " +
      s"partitions. Tune num-token-range-splits-for-scans if parallelism is low or latency is high")

    KamonShutdownHook.registerShutdownHook()

    println(spark.sparkContext.getConf.toDebugString)
//    println(spark.sparkContext.getConf
//      .get("spark.sql.shuffle.partitions"))
//    println(spark.sparkContext.getConf
//      .get("spark.executor.instances"))

//    System.exit(1)
//    System.exit(1)

    val rdd = spark.sparkContext
      .makeRDD((0 until 1000))
      .flatMap { i =>
        Kamon.init()
        KamonShutdownHook.registerShutdownHook()
        Iterator.range(1, 10000000)
          .map(j => {
            println("generating i: " + i + "; j: " + j)
            "A" * 1000000
          })
      }
      .map(s => Row.fromSeq(Seq(s)))
    val exportStartMs = System.currentTimeMillis()

    val rdd2 = rdd.filter { r =>
      r.size > 0
    }

    // NOTE: toDF(partitionCols: _*) seems buggy
    val s1 = () => spark.createDataFrame(rdd, batchExporter.exportSchema)
      .write
      .format(settings.exportFormat)
      .mode(settings.exportSaveMode)
      .options(settings.exportOptions)
      .save(settings.exportBucket)

    val s2 = () => spark.createDataFrame(rdd2, batchExporter.exportSchema)
      .write
      .format(settings.exportFormat)
      .mode(settings.exportSaveMode)
      .options(settings.exportOptions)
      .save(settings.exportBucket + "/double-woah")
//
    val e = Executors.newFixedThreadPool(2)

    val f1 = e.submit(new Runnable {
      override def run(): Unit = s1.apply()
    })
    val f2 = e.submit(new Runnable {
      override def run(): Unit = s2.apply()
    })

    f1.get()
    f2.get()

    val exportEndMs = System.currentTimeMillis()
    exportLatency.record(exportEndMs - exportStartMs)

    DownsamplerContext.dsLogger.info(s"Chunk Downsampling Driver completed successfully for downsample period " +
      s"$downsamplePeriodStr")
    val jobCompleted = Kamon.counter("chunk-migration-completed")
      .withTag("downsamplePeriod", downsamplePeriodStr)
    jobCompleted.increment()
    val downsampleHourStartGauge = Kamon.gauge("chunk-downsampler-period-start-hour")
      .withTag("downsamplePeriod", downsamplePeriodStr)
    downsampleHourStartGauge.update(userTimeStart / 1000 / 60 / 60)
    if (settings.shouldSleepForMetricsFlush)
      Thread.sleep(62000) // quick & dirty hack to ensure that the completed metric gets published
    spark
  }

}
