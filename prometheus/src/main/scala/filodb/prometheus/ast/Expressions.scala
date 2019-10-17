package filodb.prometheus.ast

import filodb.query._


trait Expressions extends Aggregates with Functions {

  case class UnaryExpression(operator: Operator, operand: Expression) extends Expression {
    //TODO Need to pass an operator to a series
  }

  case class BinaryExpression(lhs: Expression,
                              operator: Operator,
                              vectorMatch: Option[VectorMatch],
                              rhs: Expression) extends Expression with PeriodicSeries {

    operator match {
      case setOp: SetOp =>
        if (lhs.isInstanceOf[ScalarExpression] || rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("set operators not allowed in binary scalar expression")

      case comparison: Comparision if !comparison.isBool =>
        if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("comparisons between scalars must use BOOL modifier")
      case _ =>
    }
    if (vectorMatch.isDefined) {
      vectorMatch.get.validate(operator, lhs, rhs)
    }


    override def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression]) {
        throw new UnsupportedOperationException("Binary operations on scalars is not supported yet")
      }

      lhs match {
        case expression: ScalarExpression if rhs.isInstanceOf[PeriodicSeries] =>
          val scalar = ScalarFixedDoublePlan(expression.toScalar, filodb.query.TimeStepParams(timeParams.start, timeParams.step, timeParams.end))
          val seriesPlan = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = true)
        case series: PeriodicSeries if rhs.isInstanceOf[ScalarExpression] =>
          val scalar = ScalarFixedDoublePlan(rhs.asInstanceOf[ScalarExpression].toScalar,filodb.query.TimeStepParams(timeParams.start, timeParams.step, timeParams.end))
          val seriesPlan = series.toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = false)
        case function: Function if function.name.equalsIgnoreCase("scalar")  && rhs.isInstanceOf[PeriodicSeries] =>
          val scalar = function.toPeriodicSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
          val seriesPlanRhs = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, scalarIsLhs = true)
        case series: PeriodicSeries if rhs.isInstanceOf[Function] && rhs.asInstanceOf[Function].name.equalsIgnoreCase("scalar") =>
          val scalar = rhs.asInstanceOf[Function].toPeriodicSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
          val seriesPlanRhs = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, scalarIsLhs = false)
        case series: PeriodicSeries if rhs.isInstanceOf[PeriodicSeries] =>
          val seriesPlanLhs = series.toPeriodicSeriesPlan(timeParams)
          val seriesPlanRhs = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          val cardinality = if (operator.getPlanOperator.isInstanceOf[SetOperator])
            Cardinality.ManyToMany
          else
            vectorMatch.map(_.cardinality.cardinality).getOrElse(Cardinality.OneToOne)

          val matcher = vectorMatch.flatMap(_.matching)
          val onLabels = matcher.filter(_.isInstanceOf[On]).map(_.labels)
          val ignoringLabels = matcher.filter(_.isInstanceOf[Ignoring]).map(_.labels)

          BinaryJoin(seriesPlanLhs, operator.getPlanOperator, cardinality, seriesPlanRhs,
            onLabels.getOrElse(Nil), ignoringLabels.getOrElse(Nil),
            vectorMatch.flatMap(_.grouping).map(_.labels).getOrElse(Nil))
      }
    }
  }


}
