package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.ShardMapper
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.store.StoreConfig
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * A macrobenchmark (IT-test level) for QueryEngine2 aggregations, in-memory only (no on-demand paging)
 * No ingestion occurs while query test is running -- this is intentional to allow for query-focused CPU
 * and memory allocation analysis.
 * Ingests a fixed amount of tsgenerator data in Prometheus schema (time, value, tags) and runs various queries.
 */
@State(Scope.Thread)
class QueryInMemoryBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import filodb.coordinator._
  import client.Client.{actorAsk, asyncAsk}
  import client.QueryCommands._
  import NodeClusterActor._

  val numShards = 2
  val numSamples = 720   // 2 hours * 3600 / 10 sec interval
  val numSeries = 100
  val startTime = System.currentTimeMillis - (3600*1000)
  val numQueries = 500       // Please make sure this number matches the OperationsPerInvocation below
  val queryIntervalMin = 55  // # minutes between start and stop
  val queryStep = 150        // # of seconds between each query sample "step"
  val spread = 1

  // TODO: move setup and ingestion to another trait
  val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf"))
  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

  // Set up Prometheus dataset in cluster, initialize in metastore
  private val dataset = TestTimeseriesProducer.dataset
  private val shardMapper = new ShardMapper(numShards)

  Await.result(cluster.metaStore.initialize(), 3.seconds)
  Await.result(cluster.metaStore.newDataset(dataset), 5.seconds)

  val storeConf = StoreConfig(ConfigFactory.parseString("""
                  | flush-interval = 1h
                  | shard-mem-size = 512MB
                  | ingestion-buffer-mem-size = 50MB
                  | groups-per-shard = 4
                  | demand-paging-enabled = false
                  """.stripMargin))
  val command = SetupDataset(dataset.ref, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }

  import monix.execution.Scheduler.Implicits.global

  // Manually pump in data ourselves so we know when it's done.
  // TODO: ingest into multiple shards
  Thread sleep 2000    // Give setup command some time to set up dataset shards etc.
  val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(startTime, numShards, numSeries,
                                          numSamples * numSeries, dataset, shardMapper, spread)
  val ingestTask = containerStream.groupBy(_._1)
                    // Asynchronously subcribe and ingest each shard
                    .mapAsync(numShards) { groupedStream =>
                      val shard = groupedStream.key
                      println(s"Starting ingest on shard $shard...")
                      val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
                        // println(s"   XXX: -->  Got ${bytes.map(_.size).sum} bytes in shard $shard")
                        val data = bytes.map { array => SomeData(RecordContainer(array), idx) }
                        Observable.fromIterable(data)
                      }
                      Task.fromFuture(
                        cluster.memStore.ingestStream(dataset.ref, shard, shardStream, global) {
                          case e: Exception => throw e })
                    }.countL.runAsync
  Await.result(producingFut, 30.seconds)
  Thread sleep 2000
  cluster.memStore.asInstanceOf[TimeSeriesMemStore].commitIndexForTesting(dataset.ref) // commit lucene index
  println(s"Ingestion ended")

  /**
   * ## ========  Queries ===========
   * They are designed to match all the time series (common case) under a particular metric and job
   */
  val queries = Seq("heap_usage{app=\"App-2\"}",  // raw time series
                    """sum(rate(heap_usage{app="App-2"}[5m]))""",
                    """quantile(0.75, heap_usage{app="App-2"})""",
                    """sum_over_time(heap_usage{app="App-2"}[5m])""")
  // val queries = Seq("""sum_over_time(heap_usage{app="App-2"}[5m])""")
  val queryTime = startTime + (5 * 60 * 1000)  // 5 minutes from start until 60 minutes from start
  val qParams = TimeStepParams(queryTime/1000, queryStep, (queryTime/1000) + queryIntervalMin*60)
  val logicalPlans = queries.map { q => Parser.queryRangeToLogicalPlan(q, qParams) }
  val queryCommands = logicalPlans.map { plan =>
    LogicalPlan2Query(dataset.ref, plan, QueryOptions(1, 100))
  }

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  // Window = 5 min and step=2.5 min, so 50% overlap
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(500)
  def fiftyPctOverlapQueries(): Unit = {
    val futures = (0 until numQueries).map { n =>
      val f = asyncAsk(coordinator, queryCommands(n % queryCommands.length))
      f.onSuccess {
        case q: QueryResult2 =>
        case e: QError       => throw new RuntimeException(s"Query error $e")
      }
      f
    }
    Await.result(Future.sequence(futures), 60.seconds)
  }

  // Now, simulate where step is larger than the window size.
  val qParams2 = TimeStepParams(queryTime/1000, queryStep * 3, (queryTime/1000) + queryIntervalMin*60)
  val logicalPlans2 = queries.map { q => Parser.queryRangeToLogicalPlan(q, qParams2) }
  val queryCommands2 = logicalPlans2.map { plan =>
    LogicalPlan2Query(dataset.ref, plan, QueryOptions(1, 100))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(500)
  def noOverlapQueries(): Unit = {
    val futures = (0 until numQueries).map { n =>
      val f = asyncAsk(coordinator, queryCommands2(n % queryCommands2.length))
      f.onSuccess {
        case q: QueryResult2 =>
        case e: QError       => throw new RuntimeException(s"Query error $e")
      }
      f
    }
    Await.result(Future.sequence(futures), 60.seconds)
  }
}