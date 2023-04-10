package filodb.core.memstore

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.binaryrecord2.RecordSchema.schemaID
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesShardStats}
import filodb.core.memstore.DownsampleIndexBootstrapper.currentThreadScheduler
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.metadata.Column.ColumnType.HistogramColumn
import filodb.core.store.{AllChunkScan, ColumnStore, PartKeyRecord, SinglePartitionScan, TimeRangeChunkScan}
import filodb.memory.format.UnsafeUtils

class RawIndexBootstrapper(colStore: ColumnStore) {

  /**
   * Bootstrap the lucene index for the shard
   * using PartKeyRecord objects read from some persistent source.
   *
   * The partId used in the lucene index is generated by invoking
   * the function provided on the threadpool requested.
   *
   * @param index the lucene index to populate
   * @param shardNum shard number
   * @param ref dataset ref
   * @param assignPartId the function to invoke to get the partitionId to be used to populate the index record
   * @return number of updated records
   */
  def bootstrapIndexRaw(index: PartKeyLuceneIndex,
                        shardNum: Int,
                        ref: DatasetRef)
                       (assignPartId: PartKeyRecord => Int): Task[Long] = {

    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    colStore.scanPartKeys(ref, shardNum)
      .map { pk =>
        val partId = assignPartId(pk)
        index.addPartKey(pk.partKey, partId, pk.startTime, pk.endTime)()
      }
      .countL
      .map { count =>
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }
}

object DownsampleIndexBootstrapper {
  val currentThreadScheduler = {
    Scheduler.apply(ExecutionContext.fromExecutor(cmd => cmd.run()))
  }
}

class DownsampleIndexBootstrapper(colStore: ColumnStore,
                                  schemas: Schemas,
                                  stats: DownsampledTimeSeriesShardStats,
                                  datasetRef: DatasetRef,
                                  downsampleConfig: DownsampleConfig) extends StrictLogging {
  val schemaHashToHistCol = schemas.schemas.values
    // Also include DS schemas
    .flatMap(schema => schema.downsample.map(Seq(_)).getOrElse(Nil) ++ Seq(schema))
    .map{ schema =>
      val histIndex = schema.data.columns.indexWhere(col => col.columnType == HistogramColumn)
      (schema.schemaHash, histIndex)
    }
    .filter{ case (_, histIndex) => histIndex > -1 }
    .toMap

  /**
   * Update metrics about the series' "shape", e.g. label/value lengths/counts, bucket counts, etc.
   */
  private def updateDataShapeStats(pk: PartKeyRecord, shardNum: Int, schema: Schema): Unit = {
    val pairs = schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
    stats.dataShapeLabelCount.record(pairs.size)
    var totalSize = 0  // add to this as we iterate label/value pairs, then record the total
    pairs.foreach{ case (key, value) =>
      stats.dataShapeKeyLength.record(key.size)
      stats.dataShapeValueLength.record(value.size)
      if (key == schema.options.metricColumn) {
        stats.dataShapeMetricLength.record(value.size)
      }
      totalSize += key.size + value.size
    }
    stats.dataShapeTotalLength.record(totalSize)
    if (schemaHashToHistCol.contains(schema.schemaHash)) {
      val bucketCount = getHistBucketCount(pk, shardNum, schema)
      stats.dataShapeBucketCount.record(bucketCount)
    }
  }

  /**
   * Given a PartKeyRecord for a histogram, returns the count of buckets in its most-recent chunk.
   */
  private def getHistBucketCount(pk: PartKeyRecord, shardNum: Int, schema: Schema): Int = {
    val rawPartFut = colStore.readRawPartitions(
        datasetRef,
        pk.endTime,
        SinglePartitionScan(pk.partKey, shardNum),
        TimeRangeChunkScan(pk.endTime, pk.endTime))  // we only want the most-recent chunk
      .headL
      .runToFuture(currentThreadScheduler)
    val readablePart = {
      val rawPart = Await.result(rawPartFut, Duration(5, TimeUnit.SECONDS))
      new PagedReadablePartition(
        schema, shard = 0, partID = 0, partData = rawPart, minResolutionMs = 1)
    }
    val histReader = {
      val info = readablePart.infos(AllChunkScan).nextInfoReader
      val histCol = schemaHashToHistCol(schema.schemaHash)
      val ptr = info.vectorAddress(colId = histCol)
      val acc = info.vectorAccessor(colId = histCol)
      readablePart.chunkReader(columnID = histCol, acc, ptr).asHistReader
    }
    histReader.buckets.numBuckets
  }

  /**
   * Same as bootstrapIndexRaw, except that we parallelize lucene update for
   * faster bootstrap of large number of index entries in downsample cluster.
   * Not doing this in raw cluster since parallel TimeSeriesPartition
   * creation requires more careful contention analysis. Bootstrap index operation
   * builds entire index from scratch
   */
  def bootstrapIndexDownsample(index: PartKeyLuceneIndex,
                               shardNum: Int,
                               ref: DatasetRef,
                               ttlMs: Long): Task[Long] = {
    val startCheckpoint = System.currentTimeMillis()
    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis() - ttlMs
    colStore.scanPartKeys(ref, shardNum)
      .filter(_.endTime > start)
      .mapParallelUnordered(Runtime.getRuntime.availableProcessors()) { pk =>
        Task.evalAsync {
          if (downsampleConfig.enableDataShapeStats) {
            try {
              val schema = schemas(schemaID(pk.partKey, UnsafeUtils.arayOffset))
              updateDataShapeStats(pk, shardNum, schema.downsample.get)
            } catch {
              case t: Throwable =>
                logger.error("swallowing exception during data-shape stats update", t)
            }
          }
          index.addPartKey(pk.partKey, partId = -1, pk.startTime, pk.endTime)(
            pk.partKey.length,
            PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk.partKey, 0, pk.partKey.length)
          )
        }
      }
      .countL
      .map { count =>
        // Ensures index is made durable to secondary store
        index.commit()
        // Note that we do not set an end time for the Synced here, instead
        // we will do it from DownsampleTimeSeriesShard
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - startCheckpoint)
        count
      }
  }

  // scalastyle:off method.length
  /**
   * Refresh index with real-time data rom colStore's raw dataset
   * @param fromHour fromHour inclusive
   * @param toHour toHour inclusive
   * @param parallelism number of threads to use to concurrently load the index
   * @param lookUpOrAssignPartId function to invoke to assign (or lookup) partId to the partKey
   *
   * @return number of records refreshed
   */
  def refreshWithDownsamplePartKeys(
                                     index: PartKeyLuceneIndex,
                                     shardNum: Int,
                                     ref: DatasetRef,
                                     fromHour: Long,
                                     toHour: Long,
                                     schemas: Schemas,
                                     parallelism: Int = Runtime.getRuntime.availableProcessors()): Task[Long] = {

    // This method needs to be invoked for updating a range of time in an existing index. This assumes the
    // Index is already present and we need to update some partKeys in it. The lookUpOrAssignPartId is expensive
    // The part keys byte array unlike in bootstrapIndexDownsample is not opaque. The part key is broken down into
    // key value pairs, looked up in index to find the already assigned partId if any. If no partId is found the next
    // available value from the counter in DownsampleTimeSeriesShard is allocated. However, since partId is an integer
    // the max value it can reach is 2^32. This is a lot of timeseries in one shard, however, with time, especially in
    // case of durable index, and in environments with high churn, partIds evicted are not reclaimed and we may
    // potentially exceed the limit requiring us to preiodically reclaim partIds, eliminate the notion of partIds or
    // comeup with alternate solutions to come up a partId which can either be a long value or some string
    // representation
    val recoverIndexLatency = Kamon.gauge("downsample-store-refresh-index-latency",
      MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    Observable.fromIterable(fromHour to toHour).flatMap { hour =>
      colStore.getPartKeysByUpdateHour(ref, shardNum, hour)
    }.mapParallelUnordered(parallelism) { pk =>
      // Same PK can be updated multiple times, but they wont be close for order to matter.
      // Hence using mapParallelUnordered
      Task.evalAsync {
        if (downsampleConfig.enableDataShapeStats) {
          try {
            val schema = schemas(schemaID(pk.partKey, UnsafeUtils.arayOffset))
            updateDataShapeStats(pk, shardNum, schema)
          } catch {
            case t: Throwable =>
              logger.error("swallowing exception during data-shape stats update", t)
          }
        }
        val downsamplePartKey = RecordBuilder.buildDownsamplePartKey(pk.partKey, schemas)
        downsamplePartKey.foreach { dpk =>
          index.upsertPartKey(dpk, partId = -1, pk.startTime, pk.endTime)(
            dpk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(dpk, 0, dpk.length)
          )
        }
      }
    }
      .countL
      .map { count =>
        // Forces sync with underlying filesystem, on problem is for initial index sync
        // its all or nothing as we do not mark partial progress, but given the index
        // update is parallel it makes sense to wait for all to be added to index
        index.commit()
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }
  // scalastyle:on method.length
}
