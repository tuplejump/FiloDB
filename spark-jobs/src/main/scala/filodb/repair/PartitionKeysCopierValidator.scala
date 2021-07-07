package filodb.repair

import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.{CassandraColumnStore, CassandraTokenRangeSplit}
import filodb.core.{DatasetRef, GlobalConfig}
import filodb.core.metadata.Schemas
import filodb.core.store.ScanSplit
import filodb.memory.format.UnsafeUtils

case class ExpandedPartKeyRecord(pkPair: Seq[(String, String)],
                                 startTime: Long,
                                 endTime: Long)

class PartitionKeysCopierValidator(sparkConf: SparkConf) extends StrictLogging {

  private def getFiloConfig(valueConf: String, filePathConf: String) = {
    val configString = sparkConf.get(valueConf, "")
    val config = if (!configString.isEmpty) {
      ConfigFactory.parseString(configString)
    } else {
      ConfigFactory.parseFile(new File(sparkConf.get(filePathConf))).withFallback(GlobalConfig.systemConfig)
    }
    config.resolve()
  }

  def datasetConfig(mainConfig: Config, datasetName: String): Config = {
    val sourceConfigPaths = mainConfig.getConfigList("inline-dataset-configs")
    sourceConfigPaths.stream()
      .filter(new util.function.Predicate[Config] {
        override def test(conf: Config): Boolean = {
          conf.getString("dataset").equals(datasetName)
        }
      })
      .findFirst()
      .orElseThrow()
  }

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  private val rawSourceConfig = getFiloConfig(
    "spark.filodb.partitionkeys.validator.source.config.value",
    "spark.filodb.partitionkeys.validator.source.config.file"
  )

  private val rawTargetConfig = getFiloConfig(
    "spark.filodb.partitionkeys.validator.target.config.value",
    "spark.filodb.partitionkeys.validator.target.config.file"
  )

  val sourceConfig = rawSourceConfig.getConfig("filodb")
  val targetConfig = rawTargetConfig.getConfig("filodb")
  val sourceCassConfig = sourceConfig.getConfig("cassandra")
  val targetCassConfig = targetConfig.getConfig("cassandra")
  val datasetName = sparkConf.get("spark.filodb.partitionkeys.validator.dataset")
  val sourceDatasetConfig = datasetConfig(sourceConfig, datasetName)
  val targetDatasetConfig = datasetConfig(targetConfig, datasetName)

  val datasetRef = DatasetRef.fromDotString(datasetName)
  val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val numOfShards: Int = sourceDatasetConfig.getInt("num-shards")
  val repairStartTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.validator.repairStartTime"))
  val repairEndTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.validator.repairEndTime"))

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")

  val sourceCassandraColStore = new CassandraColumnStore(
    sourceConfig, readSched, sourceSession, false
  )(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(
    targetConfig, readSched, targetSession, false
  )(writeSched)

  val numSplitsForScans = sourceCassConfig.getInt("num-token-range-splits-for-scans")

  private[filodb] def getSourceScanSplits = sourceCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  private[filodb] def getTargetScanSplits = targetCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  private[filodb] def getPartKeyRows(cassandraColumnStore: CassandraColumnStore,
                                     splits: Iterator[ScanSplit]): Iterator[ExpandedPartKeyRecord] = {
    var allRecords = new ListBuffer[ExpandedPartKeyRecord]()
    for (split <- splits; shard <- 0 until numOfShards) {
      val tokens = split.asInstanceOf[CassandraTokenRangeSplit].tokens
      val srcPartKeysTable = cassandraColumnStore.getOrCreatePartitionKeysTable(datasetRef, shard)
      // CQL does not support OR operator. So we need to query separately to get the timeSeries partitionKeys
      // which were born or died during the data loss period (aka repair window).
      val rowsByStartTime = srcPartKeysTable.scanRowsByStartTimeRangeNoAsync(
        tokens, repairStartTime.toEpochMilli(), repairEndTime.toEpochMilli())
      val rowsByEndTime = srcPartKeysTable.scanRowsByEndTimeRangeNoAsync(
        tokens, repairStartTime.toEpochMilli(), repairEndTime.toEpochMilli())
      // add to a Set to eliminate duplicate entries.
      val records = (rowsByStartTime.++(rowsByEndTime))
        .map(row => ExpandedPartKeyRecord(
          Schemas.gauge.partKeySchema.toStringPairs(
            row.getBytes("partKey").array(), UnsafeUtils.arayOffset),
          row.getLong("startTime"),
          row.getLong("endTime")))
      allRecords = allRecords ++ records
    }
    allRecords.toList.iterator
  }

  private[filodb] def getSourceRows(scanSplit: Iterator[ScanSplit]): Iterator[ExpandedPartKeyRecord] = {
    getPartKeyRows(sourceCassandraColStore, scanSplit)
  }

  private[filodb] def getTargetRows(scanSplit: Iterator[ScanSplit]): Iterator[ExpandedPartKeyRecord] = {
    getPartKeyRows(targetCassandraColStore, scanSplit)
  }

  def shutdown(): Unit = {
    sourceCassandraColStore.shutdown()
    targetCassandraColStore.shutdown()
  }
}

object PartitionKeysCopierValidator {

  class ByteComparator extends java.util.Comparator[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = java.util.Arrays.compareUnsigned(a, b)
  }

  val cache = new java.util.TreeMap[Array[Byte], PartitionKeysCopierValidator](new ByteComparator)

  // scalastyle: off null
  def lookup(conf: SparkConf): PartitionKeysCopierValidator = synchronized {
    // SparkConf cannot be used as a key, so serialize it instead.
    val bout = new java.io.ByteArrayOutputStream()
    val oout = new java.io.ObjectOutputStream(bout)
    oout.writeObject(conf)
    oout.close()
    val key = bout.toByteArray()

    var validator = cache.get(key)
    if (validator == null) {
      validator = new PartitionKeysCopierValidator(conf)
      cache.put(key, validator)
    }
    validator
  }

  // scalastyle: on
}

object PartitionKeysCopierValidatorMain extends App with StrictLogging {

  def getRowsAsRdd(spark: SparkSession,
                   rdd: RDD[ExpandedPartKeyRecord]): DataFrame = {
    spark.createDataFrame(rdd)
      .toDF("partKeyPairs", "startTime", "endTime")
  }

  def run(conf: SparkConf): SparkSession = {
    logger.info(s"PartitionKeysCopierValidator Spark Job Properties: ${conf.toDebugString}")
    val validator = PartitionKeysCopierValidator.lookup(conf)

    val spark = SparkSession.builder()
      .appName("FiloDBPartitionKeysCopierValidator")
      .config(conf)
      .getOrCreate()

    val sourceRows = getRowsAsRdd(
      spark,
      spark
        .sparkContext
        .makeRDD(validator.getSourceScanSplits)
        .mapPartitions(splits => PartitionKeysCopierValidator.lookup(conf).getSourceRows(splits))
    )

    val targetRows = getRowsAsRdd(
      spark,
      spark
        .sparkContext
        .makeRDD(validator.getTargetScanSplits)
        .mapPartitions(splits => PartitionKeysCopierValidator.lookup(conf).getTargetRows(splits))
    )

    val sourceDiff = sourceRows.except(targetRows)
    val targetDiff = targetRows.except(sourceRows)

    if (sourceDiff.isEmpty) {
      logger.info(s"PartitionKeysCopierValidator validated successfully with no diff.")
    } else {
      logger.info(s"PartitionKeysCopierValidator found diff! " +
        s"Source rows size: ${sourceRows.count()} " +
        s"Target rows size: ${targetRows.count()} " +
        s"Source diff size: ${sourceDiff.count()} " +
        s"Target diff size: ${targetDiff.count()} ")
      sourceDiff.show(1000, false)
      targetDiff.show(1000, false)
    }

    validator.shutdown()
    spark
  }
}
