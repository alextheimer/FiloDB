package filodb.downsampler

import java.io.File
import java.time.Instant
import com.typesafe.config.{ConfigFactory}
import org.apache.spark.{SparkConf}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.downsample.{OffHeapMemory}
import filodb.core.memstore.{TimeSeriesShardInfo}
import filodb.core.metadata.{Schemas}
import filodb.core.query._
import filodb.core.store.{StoreConfig}
import filodb.downsampler.chunk.{BatchDownsampler, BatchExporter, Downsampler, DownsamplerSettings}
import filodb.downsampler.index.{DSIndexJobSettings}
import filodb.memory.format.ZeroCopyUTF8String._

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  // Add a path here to enable export during these tests. Useful for debugging export data.
  val exportToFile = Some("file:///Users/alextheimer/Desktop/woah")
  val exportConf =
    s"""{
       |  "filodb": { "downsampler": { "data-export": {
       |    "enabled": ${exportToFile.isDefined},
       |    "key-labels": [],
       |    "bucket": "${exportToFile.getOrElse("")}",
       |    "format": "csv",
       |    "options": {
       |      "header": true,
       |      "escape": "\\"",
       |    }
       |    "groups": [
       |      {
       |        "key": [],
       |        "rules": [
       |          {
       |            "allow-filters": [],
       |            "block-filters": [],
       |            "drop-labels": []
       |          }
       |        ]
       |      }
       |    ],
       |    "path-spec": [
       |      "year", "<<YYYY>>",
       |      "month", "<<M>>",
       |      "day", "<<d>>",
       |      "_metric_", "{{__name__}}"
       |    ]
       |  }}}
       |}
       |""".stripMargin

  val baseConf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))
  val conf = ConfigFactory.parseString(exportConf).withFallback(baseConf).resolve()
  val settings = new DownsamplerSettings(conf)
  val schemas = Schemas.fromConfig(settings.filodbConfig).get
  val queryConfig = QueryConfig(settings.filodbConfig.getConfig("query"))
  val dsIndexJobSettings = new DSIndexJobSettings(settings)
  val (dummyUserTimeStart, dummyUserTimeStop) = (123, 456)
  val batchDownsampler = new BatchDownsampler(settings, dummyUserTimeStart, dummyUserTimeStop)
  val batchExporter = new BatchExporter(settings, 123, 456)

  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  val seriesTagsNaN = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8, "nan_support".utf8 -> "yes".utf8)
  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)

  val rawColStore = batchDownsampler.rawCassandraColStore
  val downsampleColStore = batchDownsampler.downsampleCassandraColStore

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram,
                                          Schemas.deltaCounter, Schemas.deltaHistogram, Schemas.untyped),
                                     Map.empty, 100, rawDataStoreConfig)
  val shardInfo = TimeSeriesShardInfo(0, batchDownsampler.shardStats,
    offheapMem.bufferPools, offheapMem.nativeMemoryManager)

  val untypedName = "my_untyped"
  var untypedPartKeyBytes: Array[Byte] = _

  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _

  val counterName = "my_counter"
  var counterPartKeyBytes: Array[Byte] = _

  val deltaCounterName = "my_delta_counter"
  var deltaCounterPartKeyBytes: Array[Byte] = _

  val histName = "my_histogram"
  val histNameNaN = "my_histogram_NaN"
  var histPartKeyBytes: Array[Byte] = _
  var histNaNPartKeyBytes: Array[Byte] = _

  val deltaHistName = "my_delta_histogram"
  val deltaHistNameNaN = "my_histogram_NaN"
  var deltaHistPartKeyBytes: Array[Byte] = _
  var deltaHistNaNPartKeyBytes: Array[Byte] = _

  val gaugeLowFreqName = "my_gauge_low_freq"
  var gaugeLowFreqPartKeyBytes: Array[Byte] = _

  val lastSampleTime = 74373042000L
  val pkUpdateHour = hour(lastSampleTime)

  val metricNames = Seq(gaugeName, gaugeLowFreqName, counterName, deltaCounterName, histName, deltaHistName, histNameNaN, untypedName)
  val shard = 0

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

  val currTime = System.currentTimeMillis()

  override def beforeAll(): Unit = {
    batchDownsampler.downsampleRefsByRes.values.foreach { ds =>
      downsampleColStore.initialize(ds, 4).futureValue
      downsampleColStore.truncate(ds, 4).futureValue
    }
    rawColStore.initialize(batchDownsampler.rawDatasetRef, 4).futureValue
    rawColStore.truncate(batchDownsampler.rawDatasetRef, 4).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    sparkConf.set("spark.sql.shuffle.partitions", "10")
    sparkConf.set("spark.executor.instances", "4")
    sparkConf.set("spark.executor.cores", "5")
    sparkConf.set("spark.driver.cores", "5")
    sparkConf.set("spark.default.parallelism", "5")
    println(sparkConf.toDebugString)
//    System.exit(1)
    val downsampler = new Downsampler(settings)
    downsampler.run(sparkConf).close()
  }
}
