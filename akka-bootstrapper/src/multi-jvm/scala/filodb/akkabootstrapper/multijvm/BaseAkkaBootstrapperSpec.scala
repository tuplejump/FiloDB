package filodb.akkabootstrapper.multijvm

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.AddressFromURIString
import akka.cluster.Cluster
import akka.http.scaladsl.Http
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import filodb.akkabootstrapper.{AkkaBootstrapper, ClusterMembershipHttpResponse}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.language.postfixOps

trait AkkaBootstrapperMultiNodeConfig extends MultiNodeConfig {

  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

  def baseConfig: Config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loggers = ["akka.event.slf4j.Slf4jLogger"]
       |  logger-startup-timeout = 30s
       |  actor.provider = "cluster"
       |  remote {
       |    log-remote-lifecycle-events = off
       |    netty.tcp.hostname = "127.0.0.1"
       |    netty.tcp.port = 0
       |  }
       |}
       |akka-bootstrapper {
       |  http-seeds.base-url = "http://127.0.0.1:8080/"
       |  seed-discovery.timeout = 1 minute
       |}
     """.stripMargin
  ).withFallback(ConfigFactory.load())

  // multijvmtest.http.port and all common config should have been set here, but it is set in
  // sub-classes because of limitation in multi-jvm testkit
}

/**
  * This is the base spec that contains test for the akka bootstrapper API contract.
  * Concrete multi-jvm tests can mixin this trait to instantiate tests for each discovery strategy.
  */
trait BaseAkkaBootstrapperSpec extends MultiNodeSpecCallbacks
  with AnyWordSpecLike with matchers.should.Matchers with ScalaFutures
  with BeforeAndAfterAll { multiNodeSpecWithConfig: MultiNodeSpec =>

  import io.circe.parser.decode
  import io.circe.generic.auto._

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit = multiNodeSpecAfterAll()

  override def initialParticipants: Int = roles.size

  val config = system.settings.config
  val cluster = Cluster(system)
  val akkaBootstrapperMultiNodeConfig: AkkaBootstrapperMultiNodeConfig

  "An application using the ConsulAkkaBootstrapper" must {

    "be able to bootstrap cluster with two members and then scale the cluster up by adding a third" in {

      import akkaBootstrapperMultiNodeConfig._

      runOn(node3) {
        enterBarrier("initialClusterBootstrapped")
      }
      val bootstrapper = AkkaBootstrapper(cluster)
      bootstrapper.bootstrap()
      implicit val materializer = ActorMaterializer()
      implicit val executionContext = system.dispatcher
      val httpPort = config.getInt("multijvmtest.http.port")
      val binding = Http().bindAndHandle(bootstrapper.getAkkaHttpRoute(), "127.0.0.1", httpPort).futureValue
      val seedsEndpoint = s"http:/${binding.localAddress}/__members"
      runOn(node1, node2) {
        awaitCond(cluster.state.members.size == 2, max = 10 seconds, interval = 1 second)
        validateSeedsFromHttpEndpoint(seedsEndpoint, 2)
        enterBarrier("initialClusterBootstrapped")
      }
      enterBarrier("thirdMemberAdded")
      awaitCond(cluster.state.members.size == 3, max = 10 seconds, interval = 1 second)
      validateSeedsFromHttpEndpoint(seedsEndpoint, 3)
    }
  }

  protected def validateSeedsFromHttpEndpoint(seedsEndpoint: String, numSeeds: Int): Unit = {
    Thread.sleep(4000) // sleep for a bit for ClusterMembershipTracker actor to get the message
    val response = scalaj.http.Http(seedsEndpoint).timeout(500, 500).asString
    response.is2xx shouldEqual true
    val addresses = decode[ClusterMembershipHttpResponse](response.body).right.get
                      .members.map(a => AddressFromURIString.parse(a))
    addresses.size shouldEqual numSeeds
  }
}

