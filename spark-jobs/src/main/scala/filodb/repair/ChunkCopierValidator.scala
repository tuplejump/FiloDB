package filodb.repair

import java.io.File
import java.nio.ByteBuffer
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.{CassandraColumnStore, CassandraTokenRangeSplit, TimeSeriesChunksTable}
import filodb.core.{DatasetRef, GlobalConfig}
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSetInfoOnHeap, ScanSplit}

case class ChunkRecord(partition: String,
                       chunkId: Long,
                       infoIngestionTime: Long,
                       infoNumRows: Int,
                       infoStartTime: Long,
                       infoEndTime: Long)

class ChunkCopierValidator(sparkConf: SparkConf) extends StrictLogging {
  // Get filo config from spark conf or file.
  private def getFiloConfig(valueConf: String, filePathConf: String) = {
    val configString = sparkConf.get(valueConf, "")
    val config = if (!configString.isEmpty) {
      ConfigFactory.parseString(configString).resolve()
    } else {
      ConfigFactory.parseFile(new File(sparkConf.get(filePathConf)))
        .withFallback(GlobalConfig.systemConfig)
        .resolve()
    }
    config
  }

  def datasetConfig(mainConfig: Config, datasetName: String): Config = {
    val sourceConfigPaths = mainConfig.getConfigList("inline-dataset-configs")
    sourceConfigPaths.stream()
      .filter(new util.function.Predicate[Config] {
        override def test(conf: Config): Boolean = conf.getString("dataset").equals(datasetName)
      })
      .findFirst()
      .orElseThrow()
  }

  val rawSourceConfig = getFiloConfig(
    "spark.filodb.chunks.copier.validator.source.config.value",
    "spark.filodb.chunks.copier.validator.source.config.file"
  )
  val rawTargetConfig = getFiloConfig(
    "spark.filodb.chunks.copier.validator.target.config.value",
    "spark.filodb.chunks.copier.validator.target.config.file"
  )
  val sourceConfig = rawSourceConfig.getConfig("filodb")
  val targetConfig = rawTargetConfig.getConfig("filodb")
  val sourceCassConfig = sourceConfig.getConfig("cassandra")
  val targetCassConfig = targetConfig.getConfig("cassandra")
  val datasetName = sparkConf.get("spark.filodb.chunks.copier.validator.dataset")
  val datasetRef = DatasetRef.fromDotString(datasetName)
  val targetDatasetConfig = datasetConfig(targetConfig, datasetName)

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  val ingestionTimeStart = parseDateTime(sparkConf.get("spark.filodb.chunks.copier.validator.repairStartTime"))
  val ingestionTimeEnd = parseDateTime(sparkConf.get("spark.filodb.chunks.copier.validator.repairEndTime"))

  val numSplitsForScans = sourceCassConfig.getInt("num-token-range-splits-for-scans")
  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")

  val sourceSession = FiloSessionProvider.openSession(sourceCassConfig)
  val targetSession = FiloSessionProvider.openSession(targetCassConfig)

  val sourceCassandraColStore = new CassandraColumnStore(
  sourceConfig, readSched, sourceSession, false)(writeSched)
  val targetCassandraColStore = new CassandraColumnStore(
  targetConfig, readSched, targetSession, false)(writeSched)

  private[filodb] def getSourceScanSplits = sourceCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  private[filodb] def getTargetScanSplits = targetCassandraColStore.getScanSplits(datasetRef, numSplitsForScans)

  def getRows(chunksTable: TimeSeriesChunksTable,
              partition: ByteBuffer,
              chunkInfos: ArrayBuffer[ByteBuffer]): Iterator[ChunkRecord] = {

    val partSchema = Schemas.fromConfig(sourceConfig).get.part

    chunksTable.readChunksNoAsync(partition, chunkInfos)
      .iterator()
      .asScala
      .map(
        row => {
          val partitionString = partSchema.binSchema.stringify(partition.array())
          val info = ChunkSetInfoOnHeap(row.getBytes(1).array(), Array.empty)
          ChunkRecord(
            partitionString,
            row.getLong(0),
            info.ingestionTime,
            info.numRows,
            info.startTime,
            info.endTime
          )
        }
      )
  }

  private[filodb] def getChunks(cassandraColumnStore: CassandraColumnStore,
                                splits: Iterator[ScanSplit]): Iterator[ChunkRecord] = {
    val chunkInfos = new ArrayBuffer[ByteBuffer]()
    val indexTable = cassandraColumnStore.getOrCreateIngestionTimeIndexTable(datasetRef)
    val chunksTable = cassandraColumnStore.getOrCreateChunkTable(datasetRef)

    var allRecords = new ListBuffer[ChunkRecord]()

    // scalastyle:off null
    var lastPartition: ByteBuffer = null

    for (split <- splits) {
      val tokens = split.asInstanceOf[CassandraTokenRangeSplit].tokens
      val rows = indexTable.scanRowsByIngestionTimeNoAsync(
        tokens,
        ingestionTimeStart.toEpochMilli(),
        ingestionTimeEnd.toEpochMilli()
      )
      for (row <- rows) {
        val partition = row.getBytes(0) // partition

        if (!partition.equals(lastPartition)) {
          if (lastPartition != null) {
            allRecords = allRecords ++ getRows(chunksTable, lastPartition, chunkInfos)
            chunkInfos.clear()
          }
          lastPartition = partition;
        }

        chunkInfos += row.getBytes(3) // info

        if (chunkInfos.size >= 10000) {
          allRecords = allRecords ++ getRows(chunksTable, lastPartition, chunkInfos)
          chunkInfos.clear()
        }
      }
    }

    if (lastPartition != null) {
      allRecords = allRecords ++ getRows(chunksTable, lastPartition, chunkInfos)
      chunkInfos.clear()
    }
    allRecords.toList.iterator
  }

  private[filodb] def getSourceRows(scanSplit: Iterator[ScanSplit]): Iterator[ChunkRecord] = {
    getChunks(sourceCassandraColStore, scanSplit)
  }

  private[filodb] def getTargetRows(scanSplit: Iterator[ScanSplit]): Iterator[ChunkRecord] = {
    getChunks(targetCassandraColStore, scanSplit)
  }

  def shutdown(): Unit = {
    sourceCassandraColStore.shutdown()
    targetCassandraColStore.shutdown()
  }
}

object ChunkCopierValidator {
  class ByteComparator extends java.util.Comparator[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = java.util.Arrays.compareUnsigned(a, b)
  }

  val cache = new java.util.TreeMap[Array[Byte], ChunkCopierValidator](new ByteComparator)

  // scalastyle: off null
  def lookup(conf: SparkConf): ChunkCopierValidator = synchronized {
    // SparkConf cannot be used as a key, so serialize it instead.
    val bout = new java.io.ByteArrayOutputStream()
    val oout = new java.io.ObjectOutputStream(bout)
    oout.writeObject(conf)
    oout.close()
    val key = bout.toByteArray()

    var copier = cache.get(key)
    if (copier == null) {
      copier = new ChunkCopierValidator(conf)
      cache.put(key, copier)
    }
    copier
  }
  // scalastyle: on
}

object ChunkCopierValidatorMain extends App with StrictLogging {
  def getRowsAsRdd(spark: SparkSession,
                   rdd: RDD[ChunkRecord]): DataFrame = {
    spark.createDataFrame(rdd)
      .toDF("partition", "chunkId", "infoIngestionTime", "infoNumRows", "infoStartTime", "infoEndTime")
  }

  def run(conf: SparkConf): SparkSession = {
    logger.info(s"ChunkCopierValidator Spark Job Properties: ${conf.toDebugString}")
    val validator = ChunkCopierValidator.lookup(conf)

    val spark = SparkSession.builder()
      .appName("FiloDBChunkCopierValidator")
      .config(conf)
      .getOrCreate()

    val sourceRows = getRowsAsRdd(
      spark,
      spark
        .sparkContext
        .makeRDD(validator.getSourceScanSplits)
        .mapPartitions(splits => ChunkCopierValidator.lookup(conf).getSourceRows(splits))
    )

    val targetRows = getRowsAsRdd(
      spark,
      spark
        .sparkContext
        .makeRDD(validator.getTargetScanSplits)
        .mapPartitions(splits => ChunkCopierValidator.lookup(conf).getTargetRows(splits))
    )

    val sourceDiff = sourceRows.except(targetRows)
    val targetDiff = targetRows.except(sourceRows)

    if (sourceDiff.isEmpty) {
      logger.info(s"ChunkCopierValidator validated successfully with no diff.")
    } else {
      logger.info(s"ChunkCopierValidator found diff! " +
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