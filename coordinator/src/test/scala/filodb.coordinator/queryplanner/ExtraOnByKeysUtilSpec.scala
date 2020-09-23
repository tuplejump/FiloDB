package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{Aggregate, BinaryJoin}

class ExtraOnByKeysUtilSpec extends AnyFunSpec with Matchers {

  import ExtraOnByKeysUtil._

  val extraKeysTimeRange = Seq(Seq(25000000L, 30000000L))

  it("should add extra by keys for aggregate when no keys present") {
    val lp = Parser.queryRangeToLogicalPlan("""sum(rate(foo[5m]))""", TimeStepParams(20000, 100, 30000))
    getRealByLabels(lp.asInstanceOf[Aggregate], extraKeysTimeRange) shouldEqual ExtraOnByKeysUtil.extraByOnKeys
  }

  it("should not add extra by keys for aggregate when without present") {
    val lp = Parser.queryRangeToLogicalPlan("""sum(rate(foo[5m])) without (pod)""",
                               TimeStepParams(20000, 100, 30000))
    getRealByLabels(lp.asInstanceOf[Aggregate], extraKeysTimeRange) shouldEqual Seq.empty
  }

  it("should add extra by keys for aggregate when on already keys present") {
    val lp = Parser.queryRangeToLogicalPlan("""sum(rate(foo[5m])) by (pod)""",
      TimeStepParams(20000, 100, 30000))
    getRealByLabels(lp.asInstanceOf[Aggregate], extraKeysTimeRange) shouldEqual Seq("pod", "_pi_", "_step_")
  }

  it("should add extra on keys for binary join when no keys present") {
    val lp = Parser.queryRangeToLogicalPlan("""foo + bar """,
      TimeStepParams(20000, 100, 30000))
    getRealOnLabels(lp.asInstanceOf[BinaryJoin], extraKeysTimeRange) shouldEqual ExtraOnByKeysUtil.extraByOnKeys
  }

  it("should add extra on keys for binary join when on already keys present") {
    val lp = Parser.queryRangeToLogicalPlan("""foo + on(pod) bar """,
      TimeStepParams(20000, 100, 30000))
    getRealOnLabels(lp.asInstanceOf[BinaryJoin], extraKeysTimeRange) shouldEqual Seq("pod", "_pi_", "_step_")
  }

  it("should not add extra on keys for binary join when ignoring present") {
    val lp = Parser.queryRangeToLogicalPlan("""foo + ignoring(pod) bar """,
      TimeStepParams(20000, 100, 30000))
    getRealOnLabels(lp.asInstanceOf[BinaryJoin], extraKeysTimeRange) shouldEqual Seq.empty
  }

  it("should add extra keys even with overlap is inside of the first lookback range") {
    val lp = Parser.queryRangeToLogicalPlan("""sum(rate(foo[5m]))""", TimeStepParams(30005L, 100, 40000))
    getRealByLabels(lp.asInstanceOf[Aggregate], extraKeysTimeRange) shouldEqual ExtraOnByKeysUtil.extraByOnKeys
  }

}
