package filodb.coordinator

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._

object NodeClusterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  // Uses our common Akka test config from application_test.conf
  val globalConfig = ConfigFactory.load("application_test.conf")
  commonConfig(globalConfig)
}

/**
 * Tests the NodeClusterActor cluster singleton, dataset setup error responses,
 * and shard assignment changes when nodes join and leave
 */
abstract class NodeClusterSpec extends MultiNodeSpec(NodeClusterSpecConfig)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with CoordinatorSetupWithFactory
  with ImplicitSender
  with ScalaFutures {

  import akka.testkit._
  import NodeClusterSpecConfig._
  import NodeClusterActor._
  import NamesTestData._

  override def initialParticipants = roles.size

  override def beforeAll() = {
    metaStore.clearAllData().futureValue
    multiNodeSpecBeforeAll()

    // Initialize dataset
    metaStore.newDataset(dataset).futureValue should equal (Success)
    schema.foreach { col => metaStore.newColumn(col, datasetRef).futureValue should equal (Success) }
  }

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")

  val address1 = node(first).address
  val address2 = node(second).address

  var clusterActor: ActorRef = _

  it("should start NodeClusterActor, CoordActors and join one node") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor

    runOn(first) {
      cluster join address1
      awaitCond(cluster.state.members.size == 1)

      clusterActor = singletonClusterActor("worker")
    }
    enterBarrier("first-node-joined-cluster-actor-started")
  }

  it("should get roles back or NoSuchRole") {
    runOn(first) {
      clusterActor ! GetRefs("first")
      expectMsg(30.seconds, NoSuchRole)

      clusterActor ! GetRefs("worker")
      expectMsg(Set(coordinatorActor))
    }
  }

  val spec = DatasetResourceSpec(4, 2)   // 4 shards, 2 nodes, 2 shards per node

  it("should return UnknownDataset when dataset missing or no columns defined") {
    runOn(first) {
      clusterActor ! SubscribeShardUpdates(datasetRef)
      expectMsg(UnknownDataset(datasetRef))

      clusterActor ! SetupDataset(DatasetRef("noColumns"), Seq("first"), spec, noOpSource)
      expectMsg(UnknownDataset(DatasetRef("noColumns")))
    }
  }

  it("should return UndefinedColumns if trying to setup with undefined columns") {
    runOn(first) {
      clusterActor ! SetupDataset(datasetRef, Seq("first", "country"), spec, noOpSource)
      expectMsg(UndefinedColumns(Set("country")))
    }
    enterBarrier("end-of-undefined-columns")
  }

  // NOTE: getting a BadSchema should never happen.  Creating a dataset with either CLI or Spark,
  // all of the columns are checked.

  it("should setup dataset on all nodes for valid dataset and get ShardMap updates") {
    runOn(first) {
      clusterActor ! SetupDataset(datasetRef, Seq("first", "last", "age", "seg"), spec, noOpSource)
      expectMsg(DatasetVerified)

      clusterActor ! SubscribeShardUpdates(datasetRef)
      val shardMap = expectMsgPF(3.seconds.dilated) {
        case ShardMapUpdate(datasetRef, newMap) => newMap
      }
      shardMap.numAssignedShards should equal (2)
      shardMap.allNodes should equal (Set(coordinatorActor))

      // Calling SetupDataset again should return DatasetAlreadySetup
      clusterActor ! SetupDataset(datasetRef, Seq("first", "age"), spec, noOpSource)
      expectMsg(DatasetAlreadySetup(datasetRef))
    }

    enterBarrier("dataset-setup")
  }

  it("should get ShardMap updates and have shards assigned when new node joins") {
    runOn(second) {
      cluster join address1
      awaitCond(cluster.state.members.size == 2)
      clusterActor = singletonClusterActor("worker")
    }

    enterBarrier("second-node-joined")

    runOn(first) {
      val shardMap = expectMsgPF(3.seconds.dilated) {
        case ShardMapUpdate(datasetRef, newMap) => newMap
      }
      shardMap.numAssignedShards should equal (4)
      shardMap.allNodes.size should equal (2)
    }

    enterBarrier("second-node-update-received")
  }

  it("should update PartitionMap if node goes down") (pending)
}

class NodeClusterSpecMultiJvmNode1 extends NodeClusterSpec
class NodeClusterSpecMultiJvmNode2 extends NodeClusterSpec