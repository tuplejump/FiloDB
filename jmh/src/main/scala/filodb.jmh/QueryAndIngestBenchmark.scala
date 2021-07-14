package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}

import filodb.core.GlobalConfig
import filodb.core.SpreadChange
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.query.QueryContext
import filodb.core.store.StoreConfig
import filodb.gateway.GatewayServer
import filodb.gateway.conversion.PrometheusInputRecord
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * A macrobenchmark (IT-test level) for QueryEngine2 aggregations, in-memory only (no on-demand paging)
 * Ingestion runs while query is running too to measure impact of ingestion on querying.
 * Note that some CPU is needed for generating the pseudorandom input data, so use this benchmark not as
 * an absolute but as a relative benchmark to test impact of changes.
 * Ingests a fixed amount of tsgenerator data in Prometheus schema (time, value, tags) and runs various queries.
 */
@State(Scope.Thread)
class QueryAndIngestBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import filodb.coordinator._
  import client.Client.{actorAsk, asyncAsk}
  import client.QueryCommands._
  import NodeClusterActor._

  val numShards = 2
  val numSamples = 1080   // 3 hours * 3600 / 10 sec interval
  val numSeries = 800
  var startTime = System.currentTimeMillis - (3600*1000)
  val numQueries = 200        // Please make sure this number matches the OperationsPerInvocation below
  val queryIntervalMin = 180  // # minutes between start and stop
  val queryStep = 60          // # of seconds between each query sample "step"
  val spread = 1

  // TODO: move setup and ingestion to another trait
  val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf")
    .withValue("filodb.memstore.ingestion-buffer-mem-size", ConfigValueFactory.fromAnyRef("50MB")))
  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

  // Set up Prometheus dataset in cluster, initialize in metastore
  private val dataset = TestTimeseriesProducer.dataset
  private val shardMapper = new ShardMapper(numShards)

  Await.result(cluster.metaStore.initialize(), 3.seconds)

  val storeConf = StoreConfig(ConfigFactory.parseString("""
                  | flush-interval = 10s     # Ensure regular flushes so we can clear out old blocks
                  | shard-mem-size = 512MB
                  | groups-per-shard = 4
                  | demand-paging-enabled = false
                  """.stripMargin))
  val command = SetupDataset(dataset, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }
  coordinator ! command

  import monix.execution.Scheduler.Implicits.global

  // Manually pump in data ourselves so we know when it's done.
  // TODO: ingest into multiple shards
  Thread sleep 2000    // Give setup command some time to set up dataset shards etc.

  val (shardQueues, containerStream) = GatewayServer.shardingPipeline(GlobalConfig.systemConfig, numShards, dataset)

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
                        cluster.memStore.ingestStream(dataset.ref, shard, shardStream, global))
                    }.countL.runAsync

  val memstore = cluster.memStore.asInstanceOf[TimeSeriesMemStore]
  val shards = (0 until numShards).map { s => memstore.getShardE(dataset.ref, s) }

  private def ingestSamples(noSamples: Int): Future[Unit] = Future {
    TestTimeseriesProducer.timeSeriesData(startTime, numSeries)
      .take(noSamples * numSeries)
      .foreach { rec =>
        // we shouldn't ingest samples for same timestamps repeatedly. This will also result in out-of-order samples.
        startTime = Math.max(startTime, rec.asInstanceOf[PrometheusInputRecord].timestamp + 10000)
        val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
        while (!shardQueues(shard).offer(rec)) { Thread sleep 50 }
      }
  }

  // Initial ingest just to populate index
  Await.result(ingestSamples(30), 30.seconds)
  Thread sleep 2000
  memstore.refreshIndexForTesting(dataset.ref) // commit lucene index
  println(s"Initial ingestion ended, indexes set up")

  /**
   * ## ========  Queries ===========
   * They are designed to match all the time series (common case) under a particular metric and job
   */
  val queries = Seq("heap_usage{_ws_=\"demo\",_ns_=\"App-2\"}",  // raw time series
                    """sum(rate(heap_usage{_ws_="demo",_ns_="App-2"}[1m]))""",
                    """quantile(0.75, heap_usage{_ws_="demo",_ns_="App-2"})""",
                    """sum_over_time(heap_usage{_ws_="demo",_ns_="App-2"}[1m])""")
  val queryTime = startTime + (5 * 60 * 1000)  // 5 minutes from start until 60 minutes from start
  val qParams = TimeStepParams(queryTime/1000, queryStep, (queryTime/1000) + queryIntervalMin*60)
  val logicalPlans = queries.map { q => Parser.queryRangeToLogicalPlan(q, qParams) }
  val queryCommands = logicalPlans.map { plan =>
    LogicalPlan2Query(dataset.ref, plan, QueryContext(Some(new StaticSpreadProvider
    (SpreadChange(0, 1))), 1000000))
  }

  private var testProducingFut: Option[Future[Unit]] = None

  // Teardown after EVERY invocation is done already
  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  // Setup per invocation to make sure previous ingestion is finished
  @Setup(JMHLevel.Invocation)
  def setupQueries(): Unit = {
    testProducingFut.foreach { fut =>
      // Wait for producing future to end
      Await.result(fut, 30.seconds)
    }
    // Wait a bit for ingestion to really finish
    Thread sleep 1000
    shards.foreach(_.reclaimAllBlocksTestOnly())
    testProducingFut = Some(ingestSamples(numSamples))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(200)
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
