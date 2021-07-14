package filodb.query.exec.rangefn

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query.{exec, QueryResult, ScalarFunctionId}
import filodb.query.exec.TimeScalarGeneratorExec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ScalarFunctionSpec extends AnyFunSpec with Matchers with ScalaFutures {
  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    options = DatasetOptions(Seq("__name__", "job"), "__name__")).get

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))


  val testKey1 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

  val testKey2 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

  val testSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 1d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 5d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3d),
        new TransientRow(2L, 3d),
        new TransientRow(3L, 3d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 2d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 4d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 6d)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 0d)).iterator
      override def outputRange: Option[RvRange] = None
    })

  val oneSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 1d),
        new TransientRow(2L, 10d),
        new TransientRow(3L, 30d)
      ).iterator
      override def outputRange: Option[RvRange] = None
    })
  
  it("should generate scalar") {
    val scalarFunctionMapper = exec.ScalarFunctionMapper(ScalarFunctionId.Scalar, RangeParams(1,1,1))
    val resultObs = scalarFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultRangeVectors = resultObs.toListL.runAsync.futureValue
    resultRangeVectors.forall(x => x.isInstanceOf[ScalarFixedDouble]) shouldEqual (true)
    val resultRows = resultRangeVectors.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.size shouldEqual (1)
    resultRows.head.isNaN shouldEqual true
  }

  it("should generate scalar values when there is one range vector") {
    val scalarFunctionMapper = exec.ScalarFunctionMapper(ScalarFunctionId.Scalar, RangeParams(1,1,1))
    val resultObs = scalarFunctionMapper(Observable.fromIterable(oneSample), querySession, 1000, resultSchema, Nil)
    val resultRangeVectors = resultObs.toListL.runAsync.futureValue
    resultRangeVectors.forall(x => x.isInstanceOf[ScalarVaryingDouble]) shouldEqual (true)
    val resultRows = resultRangeVectors.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(1, 10, 30))
  }

  it("should generate time scalar") {
    val execPlan = TimeScalarGeneratorExec(QueryContext(), timeseriesDataset.ref, RangeParams(10, 10, 100), ScalarFunctionId.Time)
    implicit val timeout: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
    import monix.execution.Scheduler.Implicits.global
    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _) => {
        val rv = response(0)
        rv.isInstanceOf[TimeScalar] shouldEqual(true)
        val res = rv.rows.map(x=>(x.getLong(0), x.getDouble(1))).toList
        List((10000,10.0), (20000,20.0), (30000,30.0), (40000,40.0), (50000,50.0), (60000,60.0),
          (70000,70.0), (80000,80.0), (90000,90.0), (100000,100.0)).sameElements(res) shouldEqual(true)
      }
    }
  }
  it("should generate hour scalar") {
    val execPlan = TimeScalarGeneratorExec(QueryContext(), timeseriesDataset.ref, RangeParams(1565627710, 10, 1565627790), ScalarFunctionId.Hour)
    implicit val timeout: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
    import monix.execution.Scheduler.Implicits.global
    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _) => {
        val rv = response(0)
        rv.isInstanceOf[HourScalar] shouldEqual(true)
        val res = rv.rows.map(x=>(x.getLong(0), x.getDouble(1))).toList
        List((1565627710000L,16.0), (1565627720000L,16.0), (1565627730000L,16.0), (1565627740000L,16.0),
          (1565627750000L,16.0), (1565627760000L,16.0), (1565627770000L,16.0), (1565627780000L,16.0), (1565627790000L,16.0))
          .sameElements(res) shouldEqual(true)
      }
    }
  }
  it("should generate DayOfWeek scalar") {
    val execPlan = TimeScalarGeneratorExec(QueryContext(), timeseriesDataset.ref, RangeParams(1583682900, 100, 1583683400), ScalarFunctionId.DayOfWeek)
    implicit val timeout: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
    import monix.execution.Scheduler.Implicits.global
    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _) => {
        val rv = response(0)
        rv.isInstanceOf[DayOfWeekScalar] shouldEqual(true)
        val res = rv.rows.map(x=>(x.getLong(0), x.getDouble(1))).toList
        List((1583682900000L,0.0), (1583683000000L,0.0), (1583683100000L,0.0), (1583683200000L,0.0),
          (1583683300000L,0.0), (1583683400000L,0.0)).sameElements(res) shouldEqual(true)
      }
    }
  }
}
