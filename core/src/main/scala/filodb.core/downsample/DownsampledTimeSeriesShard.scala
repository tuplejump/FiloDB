package filodb.core.downsample

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, QuerySession}
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._

class DownsampledTimeSeriesShardStats(dataset: DatasetRef, shardNum: Int) {
  val tags = Map("shard" -> shardNum.toString, "dataset" -> dataset.toString)

  val shardTotalRecoveryTime = Kamon.gauge("downsample-total-shard-recovery-time",
    MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val partitionsQueried = Kamon.counter("downsample-partitions-queried").withTags(TagSet.from(tags))
  val chunksQueried = Kamon.counter("downsample-chunks-queried").withTags(TagSet.from(tags))
  val queryTimeRangeMins = Kamon.histogram("query-time-range-minutes").withTags(TagSet.from(tags))
  val indexEntriesRefreshed = Kamon.counter("index-entries-refreshed").withTags(TagSet.from(tags))
  val indexEntriesPurged = Kamon.counter("index-entries-purged").withTags(TagSet.from(tags))
  val indexRefreshFailed = Kamon.counter("index-refresh-failed").withTags(TagSet.from(tags))
  val indexPurgeFailed = Kamon.counter("index-purge-failed").withTags(TagSet.from(tags))
  val indexEntries = Kamon.gauge("downsample-store-index-entries").withTags(TagSet.from(tags))
  val indexRamBytes = Kamon.gauge("downsample-store-index-ram-bytes").withTags(TagSet.from(tags))
  val singlePartCassFetchLatency = Kamon.histogram("single-partition-cassandra-latency",
                                        MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
  val purgeIndexEntriesLatency = Kamon.histogram("downsample-store-purge-index-entries-latency",
                                        MeasurementUnit.time.milliseconds).withTags(TagSet.from(tags))
}

class DownsampledTimeSeriesShard(rawDatasetRef: DatasetRef,
                                 val rawStoreConfig: StoreConfig,
                                 val schemas: Schemas,
                                 store: ColumnStore, // downsample colStore
                                 rawColStore: ColumnStore,
                                 shardNum: Int,
                                 filodbConfig: Config,
                                 downsampleConfig: DownsampleConfig)
                                (implicit val ioPool: ExecutionContext) extends StrictLogging {

  val creationTime = System.currentTimeMillis()
  @volatile var isReadyForQuery = false

  private val downsampleTtls = downsampleConfig.ttls
  private val downsampledDatasetRefs = downsampleConfig.downsampleDatasetRefs(rawDatasetRef.dataset)

  private val indexDataset = downsampledDatasetRefs.last
  private val indexTtlMs = downsampleTtls.last.toMillis

  private val downsampleStoreConfig = StoreConfig(filodbConfig.getConfig("downsampler.downsample-store-config"))

  private val nextPartitionID = new AtomicInteger(0)

  private val stats = new DownsampledTimeSeriesShardStats(rawDatasetRef, shardNum)

  private val partKeyIndex = new PartKeyLuceneIndex(indexDataset, schemas.part, shardNum, indexTtlMs)

  private val indexUpdatedHour = new AtomicLong(0)

  private val indexBootstrapper = new IndexBootstrapper(store) // used for initial index loading

  private val housekeepingSched = Scheduler.computation(
    name = "housekeeping",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in Housekeeping Scheduler", _)))

  // used for periodic refresh of index, happens from raw tables
  private val indexRefresher = new IndexBootstrapper(rawColStore)

  private var houseKeepingFuture: CancelableFuture[Unit] = _
  private var gaugeUpdateFuture: CancelableFuture[Unit] = _

  def indexNames(limit: Int): Seq[String] = Seq.empty

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    LabelValueResultIterator(partKeyIndex.partIdsFromFilters(filter, startTime, endTime), labelNames, limit)
  }

  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          fetchFirstLastSampleTimes: Boolean,
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    partKeyIndex.partKeyRecordsFromFilters(filter, startTime, endTime).iterator.take(limit).map { pk =>
      val partKey = PartKeyWithTimes(pk.partKey, UnsafeUtils.arayOffset, pk.startTime, pk.endTime)
      schemas.part.binSchema.toStringPairs(partKey.base, partKey.offset).map(pair => {
        pair._1.utf8 -> pair._2.utf8
      }).toMap ++
        Map("_type_".utf8 -> Schemas.global.schemaName(RecordSchema.schemaID(partKey.base, partKey.offset)).utf8)
    }
  }

  private def hour(millis: Long = System.currentTimeMillis()) = millis / 1000 / 60 / 60

  def recoverIndex(): Future[Unit] = {
    indexBootstrapper.bootstrapIndexDownsample(partKeyIndex, shardNum, indexDataset){ _ => createPartitionID() }
      .map { count =>
        logger.info(s"Bootstrapped index for dataset=$indexDataset shard=$shardNum with $count records")
      }.map { _ =>
        // need to start recovering 6 hours prior to now since last index migration could have run 6 hours ago
        // and we'd be missing entries that would be migrated in the last 6 hours.
        // Hence indexUpdatedHour should be: currentHour - 6
        val indexJobIntervalInHours = (downsampleStoreConfig.maxChunkTime.toMinutes + 59) / 60 // for ceil division
        indexUpdatedHour.set(hour() - indexJobIntervalInHours - 1)
        stats.shardTotalRecoveryTime.update(System.currentTimeMillis() - creationTime)
        startHousekeepingTask()
        startStatsUpdateTask()
        logger.info(s"Shard now ready for query dataset=$indexDataset shard=$shardNum")
        isReadyForQuery = true
      }.runAsync(housekeepingSched)
  }

  private def startHousekeepingTask(): Unit = {
    // Run index refresh at same frequency of raw dataset's flush interval.
    // This is important because each partition's start/end time can be updated only once
    // in cassandra per flush interval. Less frequent update can result in multiple events
    // per partKey, and order (which we have  not persisted) would become important.
    // Also, addition of keys to index can be parallelized using mapAsync below only if
    // we are sure that in one raw dataset flush period, we wont get two updated part key
    // records with same part key. This is true since we update part keys only once per flush interval in raw dataset.
    logger.info(s"Starting housekeeping for downsample cluster of dataset=$rawDatasetRef shard=$shardNum " +
                s"every ${rawStoreConfig.flushInterval}")
    houseKeepingFuture = Observable.intervalWithFixedDelay(rawStoreConfig.flushInterval,
                                                           rawStoreConfig.flushInterval).mapAsync { _ =>
      purgeExpiredIndexEntries()
      indexRefresh()
    }.map { _ =>
      partKeyIndex.refreshReadersBlocking()
    }.onErrorRestartUnlimited.completedL.runAsync(housekeepingSched)
  }

  private def purgeExpiredIndexEntries(): Unit = {
    val start = System.currentTimeMillis()
    try {
      val partsToPurge = partKeyIndex.partIdsEndedBefore(System.currentTimeMillis() - downsampleTtls.last.toMillis)
      partKeyIndex.removePartKeys(partsToPurge)
      logger.info(s"Purged ${partsToPurge.length} entries from downsample " +
        s"index of dataset=$rawDatasetRef shard=$shardNum")
      stats.indexEntriesPurged.increment(partsToPurge.length)
    } catch { case e: Exception =>
      logger.error(s"Error occurred when purging index entries dataset=$rawDatasetRef shard=$shardNum", e)
      stats.indexPurgeFailed.increment()
    } finally {
      stats.purgeIndexEntriesLatency.record(System.currentTimeMillis() - start)
    }
  }

  private def indexRefresh(): Task[Unit] = {
    // Update keys until hour()-2 hours ago. hour()-1 hours ago can cause missed records if
    // refresh was triggered exactly at end of the hour. All partKeys for the hour would need to be flushed
    // before refresh happens because we will not revist the hour again.
    val toHour = hour() - 2
    val fromHour = indexUpdatedHour.get() + 1
    indexRefresher.refreshWithDownsamplePartKeys(partKeyIndex, shardNum, rawDatasetRef,
                                                 fromHour, toHour, schemas)(lookupOrCreatePartId)
      .map { count =>
        indexUpdatedHour.set(toHour)
        stats.indexEntriesRefreshed.increment(count)
        logger.info(s"Refreshed downsample index with new records numRecords=$count " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour")
      }
      .onErrorHandle { e =>
        stats.indexRefreshFailed.increment()
        logger.error(s"Error occurred when refreshing downsample index " +
          s"dataset=$rawDatasetRef shard=$shardNum fromHour=$fromHour toHour=$toHour", e)
      }
  }

  private def startStatsUpdateTask(): Unit = {
    logger.info(s"Starting Stats Update task from raw dataset=$rawDatasetRef shard=$shardNum every 1 minute")
    gaugeUpdateFuture = Observable.intervalWithFixedDelay(1.minute).map { _ =>
      updateGauges()
    }.onErrorRestartUnlimited.completedL.runAsync(housekeepingSched)
  }

  private def updateGauges(): Unit = {
    stats.indexEntries.update(partKeyIndex.indexNumEntries)
    stats.indexRamBytes.update(partKeyIndex.indexRamBytes)
  }

  private def lookupOrCreatePartId(pk: Array[Byte]): Int = {
    partKeyIndex.partIdFromPartKeySlow(pk, UnsafeUtils.arayOffset).getOrElse(createPartitionID())
  }

  /**
    * Returns a new non-negative partition ID which isn't used by any existing parition. A negative
    * partition ID wouldn't work with bitmaps.
    */
  private def createPartitionID(): Int = {
    val next = nextPartitionID.incrementAndGet()
    if (next == 0) {
      throw new IllegalStateException("Too many partitions. Reached int capacity")
    }
    next
  }

  def refreshPartKeyIndexBlocking(): Unit = {}

  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult = {
    partMethod match {
      case SinglePartitionScan(partition, _) => throw new UnsupportedOperationException
      case MultiPartitionScan(partKeys, _) => throw new UnsupportedOperationException
      case FilteredPartitionScan(split, filters) =>

        if (filters.nonEmpty) {
          // This API loads all part keys into heap and can potentially be large size for
          // high cardinality queries, but it is needed to do multiple
          // iterations over the part keys. First iteration is for data size estimation.
          // Second iteration is for query result evaluation. Loading everything to heap
          // is expensive, but we do it to handle data sizing for metrics that have
          // continuous churn. See capDataScannedPerShardCheck method.
          val recs = partKeyIndex.partKeyRecordsFromFilters(filters, chunkMethod.startTime, chunkMethod.endTime)
          if (recs.size > 100000) {
            // if we have a 3 bit spread and one shard has more than 100K tss, it means
            // that the cardinality of the query is ~800K close to a million, hence we want to log such queries
            logger.warn(s"Number of tss: ${recs.size} for query ${querySession.qContext.origQueryParams}")
          }
          val _schema = recs.headOption.map { pkRec =>
            RecordSchema.schemaID(pkRec.partKey, UnsafeUtils.arayOffset)
          }
          stats.queryTimeRangeMins.record((chunkMethod.endTime - chunkMethod.startTime) / 60000 )
          PartLookupResult(shardNum, chunkMethod, debox.Buffer.empty,
            _schema, debox.Map.empty, debox.Buffer.empty, recs, stats.chunksQueried)
        } else {
          throw new UnsupportedOperationException("Cannot have empty filters")
        }
    }
  }

  def scanPartitions(lookup: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] = {

    // Step 1: Choose the downsample level depending on the range requested
    val (resolutionMs, downsampledDataset) = chooseDownsampleResolution(lookup.chunkMethod)
    logger.debug(s"Chose resolution $downsampledDataset for chunk method ${lookup.chunkMethod}")

    capDataScannedPerShardCheck(lookup, resolutionMs)

    // Step 2: Query Cassandra table for that downsample level using downsampleColStore
    // Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.
    Observable.fromIterable(lookup.pkRecords)
      .mapAsync(downsampleStoreConfig.demandPagingParallelism) { partRec =>
        val startExecute = System.currentTimeMillis()
        // TODO test multi-partition scan if latencies are high
        store.readRawPartitions(downsampledDataset,
                                downsampleStoreConfig.maxChunkTime.toMillis,
                                SinglePartitionScan(partRec.partKey, shardNum),
                                lookup.chunkMethod)
          .map { pd =>
            val part = makePagedPartition(pd, lookup.firstSchemaId.get, resolutionMs, colIds)
            stats.partitionsQueried.increment()
            stats.singlePartCassFetchLatency.record(Math.max(0, System.currentTimeMillis - startExecute))
            part
          }
          .defaultIfEmpty(makePagedPartition(RawPartData(partRec.partKey, Seq.empty),
            lookup.firstSchemaId.get, resolutionMs, colIds))
          .headL
      }
  }

  private def capDataScannedPerShardCheck(lookup: PartLookupResult, resolution: Long) = {
    lookup.firstSchemaId.foreach { schId =>
        schemas.ensureQueriedDataSizeWithinLimit(schId, lookup.pkRecords,
                                    downsampleStoreConfig.flushInterval.toMillis,
                                    resolution, lookup.chunkMethod, downsampleStoreConfig.maxDataPerShardQuery)
    }
  }

  private def chooseDownsampleResolution(chunkScanMethod: ChunkScanMethod): (Int, DatasetRef) = {
    chunkScanMethod match {
      case AllChunkScan =>
        // pick last since it is the highest resolution
        downsampleConfig.resolutions.last.toMillis.toInt -> downsampledDatasetRefs.last
      case TimeRangeChunkScan(startTime, _) =>
        var ttlIndex = downsampleTtls.indexWhere(t => startTime > System.currentTimeMillis() - t.toMillis)
        // -1 return value means query startTime is before the earliest retention. Just pick the highest resolution
        if (ttlIndex == -1) ttlIndex = downsampleTtls.size - 1
        downsampleConfig.resolutions(ttlIndex).toMillis.toInt -> downsampledDatasetRefs(ttlIndex)
      case _ => ???
    }
  }

  private def makePagedPartition(part: RawPartData, firstSchemaId: Int,
                                 minResolutionMs: Int,
                                 colIds: Seq[Types.ColumnId]): ReadablePartition = {
    val schemaId = RecordSchema.schemaID(part.partitionKey, UnsafeUtils.arayOffset)
    if (schemaId != firstSchemaId) {
      throw SchemaMismatch(schemas.schemaName(firstSchemaId), schemas.schemaName(schemaId))
    }
    // FIXME It'd be nice to pass in the correct partId here instead of -1
    new PagedReadablePartition(schemas(schemaId), shardNum, -1, part, minResolutionMs, colIds)
  }

  /**
    * Iterator for traversal of partIds, value for the given label will be extracted from the ParitionKey.
    * this implementation maps partIds to label/values eagerly, this is done inorder to dedup the results.
    */
  case class LabelValueResultIterator(partIds: debox.Buffer[Int], labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    private lazy val rows = labelValues

    def labelValues: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
      var partLoopIndx = 0
      val rows = new mutable.HashSet[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]()
      while(partLoopIndx < partIds.length && rows.size < limit) {
        val partId = partIds(partLoopIndx)

        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata

        val currVal = schemas.part.binSchema.colValues(nextPart, UnsafeUtils.arayOffset, labelNames).
          zipWithIndex.filter(_._1 != null).map{case(value, ind) => labelNames(ind).utf8 -> value.utf8}.toMap

        if (currVal.nonEmpty) rows.add(currVal)
        partLoopIndx += 1
      }
      rows.toIterator
    }

    override def hasNext: Boolean = rows.hasNext

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = rows.next
  }

  /**
    * Retrieve partKey for a given PartId by looking up index
    */
  private def partKeyFromPartId(partId: Int): Array[Byte] = {
    val partKeyBytes = partKeyIndex.partKeyFromPartId(partId)
    if (partKeyBytes.isDefined)
      // make a copy because BytesRef from lucene can have additional length bytes in its array
      // TODO small optimization for some other day
      util.Arrays.copyOfRange(partKeyBytes.get.bytes, partKeyBytes.get.offset,
        partKeyBytes.get.offset + partKeyBytes.get.length)
    else throw new IllegalStateException(s"Could not find partKey or partId $partId. This is not a expected behavior.")
  }

  def cleanup(): Unit = {
    Option(houseKeepingFuture).foreach(_.cancel())
    Option(gaugeUpdateFuture).foreach(_.cancel())
  }

  override protected def finalize(): Unit = {
    cleanup()
  }

}
