package filodb.cli

import filodb.coordinator.{ActorName, ClusterRole, RunnableSpec}
import org.rogach.scallop.exceptions.ScallopException
import org.scalatest.Ignore

@Ignore
class FilodbCliSpec extends RunnableSpec {
  "A Filodb Cli" must {
    "initialize" in {

      testScallopOptions()
      eventually(CliMain.cluster.isInitialized)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      CliMain.role shouldEqual ClusterRole.Cli
      CliMain.system.name shouldEqual ClusterRole.Cli.systemName
      val coordinatorActor = CliMain.coordinatorActor
      coordinatorActor.path.name shouldEqual ActorName.CoordinatorName
    }

    "test hex to binary conversion, decode partkey and chunkInfo" in {
      val expectedStrPk = "b2[schema=gauge  _metric_=node_filesystem_size_bytes,tags={_ns_: us-west-2a, " +
        "_source_: mosaic, _step_: 60, _ws_: aci-kubernetes, archtype: kube, cluster: us-west-2a, " +
        "datacenter: sparks, device: nsfs, endpoint: http-metrics, fstype: nsfs, " +
        "instance: 100.80.128.89:9100, job: apc-node-exporter, mountpoint: /run/netns/cnitest-31c770b0-c6de-22bc-ba26-67e578b0c419, " +
        "namespace: apc-prometheus, node: port--10260.knode0398.usspk30a.kk.cloud.apple.com, service: kubelet}]"
      val partKeyString = CliMain.partKeyBrAsString("0x9d0100009808120000002e0000008a2a8c351a006e6f64655f66696c6573797374656d5f73697a655f62797465737101c10a0075732d776" +
        "573742d3261085f736f757263655f06006d6f73616963065f737465705f02003630c00e006163692d6b756265726e6574657308617263687479706504006b75626507636c75737465" +
        "720a0075732d776573742d32610a6461746163656e7465720600737061726b730664657669636504006e73667308656e64706f696e740c00687474702d6d65747269637306667374797" +
        "06504006e736673c412003130302e38302e3132382e38393a39313030c711006170632d6e6f64652d6578706f727465720a6d6f756e74706f696e7437002f72756e2f6e65746e73" +
        "2f636e69746573742d33316337373062302d633664652d323262632d626132362d363765353738623063343139096e616d6573706163650e006170632d70726f6d657468657573046e6f64653100706f72742d" +
        "2d31303236302e6b6e6f6465303339382e757373706b3330612e6b6b2e636c6f75642e6170706c652e636f6d077365727669636507006b7562656c6574")

      partKeyString shouldEqual expectedStrPk

      val chunkInfo = CliMain.decodeChunkInfo("0xd9dd2a5d62b595dd14000000445ae65676010000ce53f85676010000")
      chunkInfo.id shouldEqual -2479876585723077159L
      chunkInfo.numRows shouldEqual 20
      chunkInfo.startTime shouldEqual 1607774603636L
      chunkInfo.endTime shouldEqual 1607776883662L
    }

    "shutdown cleanly" in {
      CliMain.cluster.clusterActor.isEmpty shouldEqual true
      CliMain.shutdown()
      CliMain.cluster.clusterActor.isEmpty shouldEqual true
      eventually(CliMain.cluster.isTerminated)
    }
  }

  def testScallopOptions(): Unit = {

    
    parseSucessFully("--host localhost --command indexnames --dataset prometheus")
    parseSucessFully("--host localhost --port 6564 --command indexvalues --indexname asdasd --dataset prometheus --shards SS")
    parseSucessFully("""--host localhost --port 6564 --dataset "adadasd" --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --start 1212 --step 5555 --end 1212""")
    parseSucessFully("--host localhost --port 6564 --command timeseriesmetadata --matcher a=b --dataset prometheus --start 123123 --end 13123")
    parseSucessFully("--host localhost --port 6564 --command labelvalues --labelnames a --labelfilter a=b --dataset prometheus")
    parseSucessFully("""--command promFilterToPartKeyBR --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --schema prom-counter""")
    parseSucessFully("""--command partKeyBrAsString --hexpk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    parseSucessFully("""--command decodeChunkInfo --hexchunkinfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    parseSucessFully("""--command decodeVector --hexvector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectortype d""")

    parserError("""--host localhost --port 6564 --metriccolumn adasdasd --dataset "adadasd" --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --start 1231673123675123 --step 13131312313123123 --end 5""")
    parserError("""--command partKeyBrAsString --hexPk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    parserError("""--command decodeChunkInfo --hexChunkInfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    parserError("""--command decodeVector --hexVector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectortype d""")

  }

  def parseSucessFully(commandLine: String): Unit = {
    new Arguments(commandLine.split(" "))
  }

  def parserError(commandLine: String):Unit = {
    intercept[ScallopException]{
      new Arguments(commandLine.split(" "))
    }
  }
}
