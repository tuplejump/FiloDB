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
import org.openjdk.jmh.annotations.{Level => JmhLevel, _}

import filodb.core.SpreadChange
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.query.QueryContext
import filodb.core.store.StoreConfig
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * A macrobenchmark (IT-test level) for QueryEngine2 aggregations, with 100% on-demand paging from C*
 * No ingestion occurs while query test is running -- this is intentional to allow for query-focused CPU
 * and memory allocation analysis.
 * Ingests a fixed amount of tsgenerator data in Prometheus schema (time, value, tags) and runs various queries.
 */
@State(Scope.Thread)
class QueryOnDemandBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import filodb.coordinator._
  import client.Client.{actorAsk, asyncAsk}
  import client.QueryCommands._
  import NodeClusterActor._

  val numShards = 2
  val numSamples = 720   // 2 hours * 3600 / 10 sec interval
  val numSeries = 100
  val startTime = System.currentTimeMillis - (3600*1000)
  val numQueries = 10    // Should be low, so most queries actually hit C*/disk
  val queryIntervalMin = 55  // # minutes between start and stop
  val queryStep = 60         // # of seconds between each query sample "step"
  val spread = 1

  // TODO: move setup and ingestion to another trait
  val config = ConfigFactory.parseString("""
    |filodb {
    |  store-factory = "filodb.cassandra.CassandraTSStoreFactory"
    |  cassandra {
    |    hosts = "localhost"
    |    port = 9042
    |    partition-list-num-groups = 1
    |  }
    |  memstore {
    |    ingestion-buffer-mem-size = 50MB
    |  }
    |}
  """.stripMargin).withFallback(ConfigFactory.load("filodb-defaults.conf"))
  val system = ActorSystem("test", config)
  private val cluster = FilodbCluster(system)

  // Set up Prometheus dataset in cluster, initialize in metastore
  private val dataset = TestTimeseriesProducer.dataset
  private val shardMapper = new ShardMapper(numShards)

  Await.result(cluster.metaStore.initialize(), 3.seconds)
  Await.result(cluster.metaStore.clearAllData(), 5.seconds)  // to clear IngestionConfig
  Await.result(cluster.memStore.store.initialize(dataset.ref, numShards), 5.seconds)
  Await.result(cluster.memStore.store.truncate(dataset.ref, numShards), 5.seconds)

  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
  private val memStore = cluster.memStore.asInstanceOf[TimeSeriesMemStore]

  // Adjust the parameters below to test different query configs
  val storeConf = StoreConfig(ConfigFactory.parseString("""
                  | flush-interval = 1h
                  | shard-mem-size = 512MB
                  | groups-per-shard = 4
                  | multi-partition-odp = false
                  | demand-paging-parallelism = 4
                  """.stripMargin))
  val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }
  coordinator ! command

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
                      Task.fromFuture(memStore.ingestStream
                        (dataset.ref, shard, shardStream, global, Task {}))
                    }.countL.runAsync
  Await.result(producingFut, 30.seconds)
  Thread sleep 2000
  memStore.refreshIndexForTesting(dataset.ref) // commit lucene index
  println(s"Ingestion ended.")

  // For each invocation, reclaim all blocks to make sure ODP is really happening
  @Setup(JmhLevel.Invocation)
  def reclaimAll(): Unit = {
    for { shard <- 0 until numShards } {
      memStore.getShardE(dataset.ref, shard).reclaimAllBlocksTestOnly()
    }
  }

  /**
   * ## ========  Queries ===========
   * They are designed to match all the time series (common case) under a particular metric and job
   */
  val queries = Seq("heap_usage{_ns=\"App-2\"}",  // raw time series
                    """quantile(0.75, heap_usage{_ns="App-2"})""",
                    """sum(rate(heap_usage{_ns="App-1"}[5m]))""",
                    """sum_over_time(heap_usage{_ns="App-0"}[5m])""")
  val queryTime = startTime + (5 * 60 * 1000)  // 5 minutes from start until 60 minutes from start
  val qParams = TimeStepParams(queryTime/1000, queryStep, (queryTime/1000) + queryIntervalMin*60)
  val logicalPlans = queries.map { q => Parser.queryRangeToLogicalPlan(q, qParams) }
  val queryCommands = logicalPlans.map { plan =>
    LogicalPlan2Query(dataset.ref, plan, QueryContext(Some(new StaticSpreadProvider(SpreadChange(0, 1))), 20000))
  }

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(10)
  def parallelQueries(): Unit = {
    val futures = (0 until numQueries).map { n =>
      val f = asyncAsk(coordinator, queryCommands(n % queryCommands.length))
      f.foreach {
        case q: QueryResult2 =>
        case e: QError       => throw new RuntimeException(s"Query error $e")
      }
      f
    }
    Await.result(Future.sequence(futures), 60.seconds)
  }
}
