package filodb.query

import filodb.core.query.{ColumnFilter, RangeParams}

sealed trait LogicalPlan

/**
  * Super class for a query that results in range vectors with raw samples
  */
sealed trait RawSeriesPlan extends LogicalPlan

sealed trait NonLeafLogicalPlan extends LogicalPlan {
  def children: Seq[LogicalPlan]
}

/**
  * Super class for a query that results in range vectors with samples
  * in regular steps
  */
sealed trait PeriodicSeriesPlan extends LogicalPlan

sealed trait MetadataQueryPlan extends LogicalPlan

/**
  * A selector is needed in the RawSeries logical plan to specify
  * a row key range to extract from each partition.
  */
sealed trait RangeSelector extends java.io.Serializable
case object AllChunksSelector extends RangeSelector
case object WriteBufferSelector extends RangeSelector
case object InMemoryChunksSelector extends RangeSelector
case object EncodedChunksSelector extends RangeSelector
case class IntervalSelector(from: Long, to: Long) extends RangeSelector


//final case class RangeParams(start: Long, step: Long, end: Long)
/**
  * Concrete logical plan to query for raw data in a given range
  * @param columns the columns to read from raw chunks.  Note that it is not necessary to include
  *        the timestamp column, that will be automatically added.
  *        If no columns are included, the default value column will be used.
  */
case class RawSeries(rangeSelector: RangeSelector,
                     filters: Seq[ColumnFilter],
                     columns: Seq[String]) extends RawSeriesPlan

case class LabelValues(labelNames: Seq[String],
                       labelConstraints: Map[String, String],
                       lookbackTimeInMillis: Long) extends MetadataQueryPlan

case class SeriesKeysByFilters(filters: Seq[ColumnFilter],
                               start: Long,
                               end: Long) extends MetadataQueryPlan

/**
 * Concrete logical plan to query for chunk metadata from raw time series in a given range
 * @param column the column name from which to extract chunk information like chunk size and encoding type
 */
case class RawChunkMeta(rangeSelector: RangeSelector,
                        filters: Seq[ColumnFilter],
                        column: String) extends PeriodicSeriesPlan

/**
  * Concrete logical plan to query for data in a given range
  * with results in a regular time interval.
  *
  * Issue with specifying start/end/step here in the selector
  * is that plans involving multiple series can come with different
  * ranges and steps.
  *
  * This should be taken care outside this layer, or we need to have
  * proper validation.
  */
case class PeriodicSeries(rawSeries: RawSeriesPlan,
                          start: Long,
                          step: Long,
                          end: Long) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(rawSeries)
}

/**
  * Concrete logical plan to query for data in a given range
  * with results in a regular time interval.
  *
  * Applies a range function on raw windowed data before
  * sampling data at regular intervals.
  */
case class PeriodicSeriesWithWindowing(rawSeries: RawSeries,
                                       start: Long,
                                       step: Long,
                                       end: Long,
                                       window: Long,
                                       function: RangeFunctionId,
                                       functionArgs: Seq[FunctionArgsPlan] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan
{
  override def children: Seq[LogicalPlan] = Seq(rawSeries)
}

/**
  * Aggregate data across partitions (not in the time dimension).
  * Aggregation can be done only on range vectors with consistent
  * sampling interval.
  * @param by columns to group by
  * @param without columns to leave out while grouping
  */
case class Aggregate(operator: AggregationOperator,
                     vectors: PeriodicSeriesPlan,
                     params: Seq[Any] = Nil,
                     by: Seq[String] = Nil,
                     without: Seq[String] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
}

/**
  * Binary join between collections of RangeVectors.
  * One-To-One, Many-To-One and One-To-Many are supported.
  *
  * If data resolves to a Many-To-Many relationship, error will be returned.
  *
  * @param on columns to join on
  * @param ignoring columns to ignore while joining
  * @param include labels specified in group_left/group_right to be included from one side
  */
case class BinaryJoin(lhs: PeriodicSeriesPlan,
                      operator: BinaryOperator,
                      cardinality: Cardinality,
                      rhs: PeriodicSeriesPlan,
                      on: Seq[String] = Nil,
                      ignoring: Seq[String] = Nil, include: Seq[String] = Nil) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(lhs, rhs)
}

/**
  * Apply Scalar Binary operation to a collection of RangeVectors
  */
case class ScalarVectorBinaryOperation(operator: BinaryOperator,
                                       scalar: ScalarPlan,
                                       vector: PeriodicSeriesPlan,
                                       scalarIsLhs: Boolean) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vector)
}

/**
  * Apply Instant Vector Function to a collection of RangeVectors
  */
case class ApplyInstantFunction(vectors: PeriodicSeriesPlan,
                                function: InstantFunctionId,
                                functionArgs: Seq[FunctionArgsPlan] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
}

/**
  * Apply Miscellaneous Function to a collection of RangeVectors
  */
case class ApplyMiscellaneousFunction(vectors: PeriodicSeriesPlan,
                                function: MiscellaneousFunctionId,
                                      stringArgs: Seq[String] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
}

trait FunctionArgsPlan extends LogicalPlan
trait ScalarPlan extends LogicalPlan with PeriodicSeriesPlan with FunctionArgsPlan
//extends PeriodicSeriesPla
case class ScalarVaryingDoublePlan(vectors: PeriodicSeriesPlan,
                      function: ScalarFunctionId,
                      timeStepParams: RangeParams,
                      functionArgs: Seq[FunctionArgsPlan] = Nil)  extends ScalarPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
}


case class ScalarTimeBasedPlan(function: ScalarFunctionId, rangeParams: RangeParams) extends ScalarPlan  {
}

case class ScalarFixedDoublePlan(scalar: Double, timeStepParams: RangeParams) extends ScalarPlan with FunctionArgsPlan  {
}

case class VectorPlan(scalars: ScalarPlan) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(scalars)
}

/**
  * Apply Sort Function to a collection of RangeVectors
  */
case class ApplySortFunction(vectors: PeriodicSeriesPlan,
                             function: SortFunctionId) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
}

object LogicalPlan {
  /**
    * Get leaf Logical Plans
    */
  def findLeafLogicalPlans (logicalPlan: LogicalPlan) : Seq[LogicalPlan] = {
   logicalPlan match {
     // Find leaf logical plans for all children and concatenate results
     case lp: NonLeafLogicalPlan => lp.children.flatMap(findLeafLogicalPlans(_))
     case _                      => Seq(logicalPlan)
   }
  }
}