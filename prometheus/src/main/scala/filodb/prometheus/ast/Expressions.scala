package filodb.prometheus.ast

import filodb.core.query.RangeParams
import filodb.query._

case class UnaryExpression(operator: Operator, operand: Expression) extends Expression {
  //TODO Need to pass an operator to a series
}

case class PrecedenceExpression(expression: Expression) extends Expression

case class BinaryExpression(lhs: Expression,
                            operator: Operator,
                            vectorMatch: Option[VectorMatch],
                            rhs: Expression) extends Expression with PeriodicSeries {

  def validate(): Unit = {
    operator match {
      case setOp: SetOp =>
        if (lhs.isInstanceOf[ScalarExpression] || rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("set operators not allowed in binary scalar expression")

      case comparison: Comparision if !comparison.isBool =>
        if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("comparisons between scalars must use BOOL modifier")
      case _ =>
    }
  }

  if (vectorMatch.isDefined) {
    vectorMatch.get.validate(operator, lhs, rhs)
  }

  // Checks whether expression returns fixed scalar value
  def hasScalarResult(expression: Expression): Boolean = {
    expression match {
      case scalarExpression: ScalarExpression => true
      case binaryExpression: BinaryExpression => hasScalarResult(binaryExpression.lhs) &&
                                                 hasScalarResult(binaryExpression.rhs)
      case _                                  => false
    }
  }

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  override def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
    validate()

    val lhsWithPrecedence = lhs match {
      case p: PrecedenceExpression  => p.expression
      case _                        => lhs

    }

    val rhsWithPrecedence = rhs match {
      case p: PrecedenceExpression  => p.expression
      case _                        => rhs

    }

    if (hasScalarResult(lhsWithPrecedence) && hasScalarResult(rhsWithPrecedence)) {
      val rangeParams = RangeParams(timeParams.start, timeParams.step, timeParams.end)

      ((lhsWithPrecedence, rhsWithPrecedence): @unchecked) match {
        // 3 + 4
        case (lh: ScalarExpression, rh: ScalarExpression) =>
          ScalarBinaryOperation(operator.getPlanOperator, Left(lh.toScalar), Left(rh.toScalar), rangeParams)
        // (2 + 3) + 5
        case (lh: BinaryExpression, rh: ScalarExpression) => ScalarBinaryOperation(operator.getPlanOperator,
          Right(lh.toSeriesPlan(timeParams).asInstanceOf[ScalarBinaryOperation]), Left(rh.toScalar), rangeParams)
        // 2 + (3 * 5)
        case (lh: ScalarExpression, rh: BinaryExpression) => ScalarBinaryOperation(operator.getPlanOperator,
          Left(lh.toScalar), Right(rh.toSeriesPlan(timeParams).asInstanceOf[ScalarBinaryOperation]), rangeParams)
        // (2 + 3) + (5 - 6)
        case (lh: BinaryExpression, rh: BinaryExpression) => ScalarBinaryOperation(operator.getPlanOperator,
          Right(lh.toSeriesPlan(timeParams).asInstanceOf[ScalarBinaryOperation]),
          Right(rh.toSeriesPlan(timeParams).asInstanceOf[ScalarBinaryOperation]), rangeParams)
      }
    } else {
      (lhsWithPrecedence, rhsWithPrecedence) match {
        // scalar(http_requests) + scalar(node_info)
        case (lh: Function, rh: Function) if lh.isScalarFunction() && rh.isScalarFunction() =>
          val scalar = lh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
          val seriesPlanRhs = rh.toSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, true)

        // 2 + http_requests
        case (lh: ScalarExpression, rh: PeriodicSeries) =>
          val scalar = ScalarFixedDoublePlan(lh.toScalar,
            RangeParams(timeParams.start, timeParams.step, timeParams.end))
          val seriesPlan = rh.toSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = true)

        // http_requests + 2
        case (lh: PeriodicSeries, rh: ScalarExpression) =>
          val scalar = ScalarFixedDoublePlan(rh.toScalar, RangeParams(timeParams.start, timeParams.step,
            timeParams.end))
          val seriesPlan = lh.toSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = false)

        // scalar(http_requests) + node_info
        case (lh: Function, rh: PeriodicSeries) if lh.isScalarFunction() =>
          val scalar = lh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
          val seriesPlanRhs = rh.toSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, scalarIsLhs = true)

        // node_info + scalar(http_requests)
        case (lh: PeriodicSeries, rh: Function) if rh.isScalarFunction =>
          val scalar = rh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
          val seriesPlanlhs = lh.toSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanlhs, scalarIsLhs = false)

        // node_info + http_requests
        case (lh: PeriodicSeries, rh: PeriodicSeries) =>
          //10/2 + foo
          if (hasScalarResult(lh)) {
            val scalar = lh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
            val seriesPlan = rh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = true)
          } else if (hasScalarResult(rh)) { // foo + 10/2
            val scalar = rh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
            val seriesPlan = lh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = false)
          } else {
            val seriesPlanLhs = lh.toSeriesPlan(timeParams)
            val seriesPlanRhs = rh.toSeriesPlan(timeParams)
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
        case _ => throw new UnsupportedOperationException(s"Invalid operands: $lhsWithPrecedence, $rhsWithPrecedence")
      }
    }
  }
}
// scalastyle:on method.length
// scalastyle:on cyclomatic.complexity
