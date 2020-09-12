package filodb.coordinator

import java.net.Socket

import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.Ignore

import filodb.coordinator.client.MiscCommands
import filodb.core.{AbstractSpec, Success}

trait SocketChecker {
  def waitSocketOpen(port: Int): Unit = {
    while (!isPortAvail(port)) {
      println(s"Port $port is not available, waiting and retrying...")
      Thread sleep 1000
    }
  }

  def isPortAvail(port: Int): Boolean = {
    var s: Socket = null
    try {
      s = new Socket("localhost", port)
      // If the code makes it this far without an exception it means
      // something is using the port and has responded.
      return false
    } catch {
      case e: Exception => return true;
    } finally {
      if (s != null) s.close()
    }
  }
}

trait FilodbClusterNodeSpec extends AbstractSpec with ScalaFutures with FilodbClusterNode {
  val port = 22552 + util.Random.nextInt(200)

  // Ensure that CoordinatedShutdown does not shutdown the whole test JVM, otherwise Travis CI/CD fails
  override protected lazy val roleConfig = ConfigFactory.parseString(
       s"""akka.coordinated-shutdown.run-by-jvm-shutdown-hook=off
          |akka.coordinated-shutdown.exit-jvm = off
          |akka.remote.netty.tcp.port=$port
        """.stripMargin)

  implicit abstract override val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(100, Seconds)),
      interval = scaled(Span(300, Millis)))

  def assertInitialized(): Unit = {
    metaStore.initialize().futureValue shouldBe Success
    coordinatorActor
    role match {
      case ClusterRole.Cli =>
        cluster.isInitialized shouldEqual true
      case _ =>
        cluster.isInitialized shouldEqual false
        cluster.join()
        val probe = TestProbe()(system)
        val ca = cluster.clusterSingleton(role, Some(probe.ref))
        probe.expectMsgClass(classOf[NodeProtocol.PreStart])
        eventually(cluster.clusterActor.isDefined) shouldEqual true
        eventually(cluster.isInitialized) shouldEqual true
        eventually(cluster.isJoined)
    }
  }

  def assertShutdown(): Unit = {
    shutdown()
    val probe = TestProbe()(system)
    probe.awaitCond(cluster.isTerminated, cluster.settings.GracefulStopTimeout)
  }

  override def afterAll(): Unit = {
    FilodbSettings.reset()
    super.afterAll()
  }
}

// TODO disabled since several tests in this class are flaky in Travis.
@Ignore
class ClusterNodeDriverSpec extends FilodbClusterNodeSpec {
  override val role = ClusterRole.Driver

  "Driver" must {
    "not become initialized or joined until cluster singleton proxy exists on node" in {
      assertInitialized()
    }
    "eventually on shutdown become isTerminating then isTerminated and cleanly run shutdown" in {
      assertShutdown()
    }
  }
}

// TODO disabled since several tests in this class are flaky in Travis.
@Ignore
class ClusterNodeExecutorSpec extends FilodbClusterNodeSpec {
  override val role = ClusterRole.Executor

  "Executor" must {
    "have the expected config for ClusterRole.Executor" in {
      val validate = (c: Config) => akka.japi.Util.immutableSeq(
        c.getStringList("akka.cluster.roles")).forall(_ == ClusterRole.Executor.roleName) shouldEqual true

      validate(system.settings.config)
      validate(cluster.settings.allConfig)
    }
    "not become initialized or joined until cluster singleton proxy exists on node" in {
      assertInitialized()
    }
    "eventually on shutdown become isTerminating then isTerminated and cleanly run shutdown" in {
      assertShutdown()
    }
  }
}

// TODO disabled since several tests in this class are flaky in Travis.
@Ignore
class ClusterNodeServerSpec extends FilodbClusterNodeSpec {

  override val role = ClusterRole.Server

  "Server" must {
    "have the expected config for ClusterRole.Server" in {
      val roles = akka.japi.Util.immutableSeq(cluster.settings.allConfig.getStringList("akka.cluster.roles"))
      roles.size shouldEqual 1
      roles.forall(_ == ClusterRole.Server.roleName) shouldEqual true
    }
    "not become initialized or joined until cluster singleton proxy exists on node" in {
      assertInitialized()
    }
    "eventually on shutdown become isTerminating then isTerminated and cleanly run shutdown" in {
      assertShutdown()
    }
  }
}

/**
 * Initiates cluster singleton recovery sequence by populating guardian with some initial non-empty
 * shardmap and subscription state, and checking that it is recovered properly on startup
 */
class ClusterNodeRecoverySpec extends FilodbClusterNodeSpec {
  import scala.collection.immutable
  import scala.concurrent.duration._

  import akka.actor.{ActorRef, Address}
  import akka.testkit.{TestKit, TestProbe}

  import filodb.coordinator.client.LocalClient
  import filodb.core.NamesTestData._
  import NodeClusterActor._
  import NodeProtocol._

  override val role = ClusterRole.Server

  override protected lazy val roleConfig: Config = AkkaSpec.settings.allConfig

  private lazy val clusterActor = cluster.clusterSingleton(role, None)
  private lazy val client = new LocalClient(coordinatorActor)

  private val probe = TestProbe()(system)
  private val map = new ShardMapper(8)

  /** This is how FiloServer loads:
    * {{{
    *   cluster.kamonInit(role)
    *   coordinatorActor
    *   scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
    *   bootstrap(cluster.cluster)
    *   cluster.clusterSingleton(role, None)
    * }}}
    */
  "A FiloServer Node" must {
    // NOTE: we don't want to start the coordinator and clusterActor here, it has to be done later so it can be tested
    coordinatorActor
    metaStore.initialize().futureValue shouldBe Success
    cluster.joinSeedNodes(immutable.Seq.empty[Address])
    client

    "join the cluster" in {
      TestKit.awaitCond(cluster.isJoined, 10.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      // Now, pre-populate Guardian with shard state and subscribers before starting cluster actor
      // send stuff to guardian
      (0 to 3).foreach { s => map.updateFromEvent(IngestionStarted(dataset.ref, s, coordinatorActor)) }
      probe.send(cluster.guardian, CurrentShardSnapshot(dataset.ref, map))

      probe.send(cluster.guardian, GetClusterState)
      probe.expectMsgPF() {
        case ClusterState(mappers, subscriptions) =>
          mappers.size shouldEqual 1
          mappers(dataset.ref) shouldEqual map
          subscriptions.subscriptions.isEmpty shouldEqual true
      }

      // recover from Guardian
      clusterActor shouldEqual cluster.clusterActor.get

      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    // TODO: fix this test which isn't really necessary. Not sure why this fails
    // "recover proper subscribers and shard map state" in {
    //   Thread sleep 1500
    //   // check that NodeCluster/ShardCoordinator singletons have the right state too now
    //   probe.send(clusterActor, GetShardMap(dataset.ref))
    //   probe.expectMsg(CurrentShardSnapshot(dataset.ref, map))
    // }
    "shutdown cleanly" in {
      assertShutdown()
    }
  }
}
