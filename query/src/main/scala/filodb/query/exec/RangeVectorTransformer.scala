package filodb.query.exec

import monix.reactive.Observable
import scala.collection.mutable.ListBuffer
import scalaxy.loops._

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.PartitionSchema
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.memory.format.vectors.{HistogramBuckets, HistogramWithBuckets}
import filodb.query._
import filodb.query.{BinaryOperator, InstantFunctionId, MiscellaneousFunctionId, QueryConfig, SortFunctionId}
import filodb.query.InstantFunctionId.HistogramQuantile
import filodb.query.MiscellaneousFunctionId.{LabelJoin, LabelReplace}
import filodb.query.ScalarFunctionId.Scalar
import filodb.query.SortFunctionId.{Sort, SortDesc}
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn._

private case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

/**
  * Implementations can provide ways to transform RangeVector
  * results generated by ExecPlan nodes.
  *
  * Reason why these are not ExecPlan nodes themselves is because
  * they can be applied on the same node where the base RangeVectors
  * are sourced.
  *
  * It can safely be assumed that the operations in these nodes are
  * compute intensive and not I/O intensive.
  */
trait RangeVectorTransformer extends java.io.Serializable {

  def funcParams: Seq[FuncArgs]
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramsResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector]

  /**
    * Default implementation retains source schema
    */
  def schema(source: ResultSchema): ResultSchema = source

  /**
    * Args to use for the RangeVectorTransformer for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  protected[exec] def args: String
  def canHandleEmptySchemas: Boolean = false
}

object RangeVectorTransformer {
  def valueColumnType(schema: ResultSchema): ColumnType = {
    require(schema.isTimeSeries, s"Schema $schema is not time series based, cannot continue query")
    require(schema.columns.size >= 2, s"Schema $schema has less than 2 columns, cannot continue query")
    schema.columns(1).colType
  }
}

/**
  * Applies an instant vector function to every instant/row of the
  * range vectors
  */
final case class InstantVectorFunctionMapper(function: InstantFunctionId,
                                             funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String = s"function=$function"

  def evaluate(source: Observable[RangeVector], scalarRangeVector: Seq[ScalarRangeVector], queryConfig: QueryConfig,
               limit: Int, sourceSchema: ResultSchema) : Observable[RangeVector] = {
    RangeVectorTransformer.valueColumnType(sourceSchema) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function)
        if (instantFunction.isHToDoubleFunc) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new H2DoubleInstantFuncIterator(rv.rows, instantFunction.asHToDouble,
              scalarRangeVector))
          }
        } else if (instantFunction.isHistDoubleToDoubleFunc && sourceSchema.isHistDouble) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new HD2DoubleInstantFuncIterator(rv.rows, instantFunction.asHDToDouble,
              scalarRangeVector))
          }
        } else {
          throw new UnsupportedOperationException(s"Sorry, function $function is not supported right now")
        }
      case ColumnType.DoubleColumn =>
        if (function == HistogramQuantile) {
          // Special mapper to pull all buckets together from different Prom-schema time series
          val mapper = HistogramQuantileMapper(funcParams)
          mapper.apply(source, queryConfig, limit, sourceSchema, Nil)
        } else {
          val instantFunction = InstantFunction.double(function)
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new DoubleInstantFuncIterator(rv.rows, instantFunction,
              scalarRangeVector))
          }
        }
      case cType: ColumnType =>
        throw new UnsupportedOperationException(s"Column type $cType is not supported for instant functions")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    if (funcParams.isEmpty) {
      evaluate(source, Nil, queryConfig, limit, sourceSchema)
    } else {
      // Multiple ExecPlanFunArgs not supported yet
      funcParams.head match {
        case s: StaticFuncArgs   => evaluate(source, funcParams.map(x => x.asInstanceOf[StaticFuncArgs]).
                                      map(x => ScalarFixedDouble(x.timeStepParams, x.scalar)), queryConfig, limit,
                                      sourceSchema)
        case t: TimeFuncArgs     => evaluate(source, funcParams.map(x => x.asInstanceOf[TimeFuncArgs]).
                                      map(x => TimeScalar(x.timeStepParams)), queryConfig, limit, sourceSchema)
        case e: ExecPlanFuncArgs => paramResponse.head.map { param =>
                                      evaluate(source, Seq(param), queryConfig, limit, sourceSchema)
                                    }.flatten
        case _                   => throw new IllegalArgumentException(s"Invalid function param")
      }
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // if source is histogram, determine what output column type is
    // otherwise pass along the source
    RangeVectorTransformer.valueColumnType(source) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function)
        if (instantFunction.isHToDoubleFunc || instantFunction.isHistDoubleToDoubleFunc) {
          // Hist to Double function, so output schema is double
          source.copy(columns = Seq(source.columns.head, ColumnInfo("value", ColumnType.DoubleColumn)))
        } else { source }
      case cType: ColumnType          =>
        source
    }
  }
}

private class DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                        instantFunction: DoubleInstantFunction,
                                        scalar: Seq[ScalarRangeVector],
                                        result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val nextVal = next.getDouble(1)
    val timestamp = next.getLong(0)
    val newValue = instantFunction(nextVal, scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

private class H2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                          instantFunction: HistToDoubleIFunction,
                                          scalar: Seq[ScalarRangeVector],
                                          result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val timestamp = next.getLong(0)
    val newValue = instantFunction(next.getHistogram(1), scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

private class HD2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                           instantFunction: HDToDoubleIFunction,
                                           scalar: Seq[ScalarRangeVector],
                                           result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val timestamp = next.getLong(0)
    val newValue = instantFunction(next.getHistogram(1),
      next.getDouble(2), scalar.map(_.getValue(timestamp)))
    result.setValues(timestamp, newValue)
    result
  }
}

/**
  * Applies a binary operation involving a scalar to every instant/row of the
  * range vectors
  */
final case class ScalarOperationMapper(operator: BinaryOperator,
                                       scalarOnLhs: Boolean,
                                       funcParams: Seq[FuncArgs]) extends RangeVectorTransformer {
  protected[exec] def args: String = s"operator=$operator, scalarOnLhs=$scalarOnLhs"

  val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]] = Nil): Observable[RangeVector] = {
    // Multiple ExecPlanFunArgs not supported yet
    funcParams.head match {
    case s: StaticFuncArgs   => evaluate(source, ScalarFixedDouble(s.timeStepParams, s.scalar))
    case t: TimeFuncArgs     => evaluate(source, TimeScalar(t.timeStepParams))
    case e: ExecPlanFuncArgs => paramResponse.head.map(param => evaluate(source, param)).flatten
   }
  }

  private def evaluate(source: Observable[RangeVector], scalarRangeVector: ScalarRangeVector) = {
    source.map { rv =>
      val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {
        private val rows = rv.rows
        private val result = new TransientRow()

        override def hasNext: Boolean = rows.hasNext

        override def next(): RowReader = {
          val next = rows.next()
          val nextVal = next.getDouble(1)
          val timestamp = next.getLong(0)
          val sclrVal = scalarRangeVector.getValue(timestamp)
          val newValue = if (scalarOnLhs) operatorFunction.calculate(sclrVal, nextVal)
                         else operatorFunction.calculate(nextVal, sclrVal)
          result.setValues(timestamp, newValue)
          result
        }
      }
      IteratorBackedRangeVector(rv.key, resultIterator)
    }
  }
}

final case class MiscellaneousFunctionMapper(function: MiscellaneousFunctionId, funcStringParam: Seq[String] = Nil,
                                             funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams funcStringParam=$funcStringParam"

  val miscFunction: MiscellaneousFunction = {
    function match {
      case LabelReplace => LabelReplaceFunction(funcStringParam)
      case LabelJoin    => LabelJoinFunction(funcStringParam)
      case _            => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    miscFunction.execute(source)
  }
}

final case class SortFunctionMapper(function: SortFunctionId) extends RangeVectorTransformer {
  protected[exec] def args: String = s"function=$function"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    if (sourceSchema.columns(1).colType == ColumnType.DoubleColumn) {

      val ordering: Ordering[Double] = function match {
        case Sort => (Ordering[Double])
        case SortDesc => (Ordering[Double]).reverse
        case _ => throw new UnsupportedOperationException(s"$function not supported.")
      }

      val resultRv = source.toListL.map { rvs =>
        val buf = rvs.map { rv =>
          new RangeVector {
            override def key: RangeVectorKey = rv.key

            override def rows: Iterator[RowReader] = new BufferableIterator(rv.rows).buffered
          }
        }
        println("hasNext before calling head:" + buf.map((_.rows.asInstanceOf[BufferedIterator[RowReader]].hasNext)))
        println("t head:" + buf.map(_.rows.asInstanceOf[BufferedIterator[RowReader]].head.getDouble(1)))
        println("hasNext after calling head:" + buf.map((_.rows.asInstanceOf[BufferedIterator[RowReader]].hasNext)))

//          .sortBy { rv => rv.rows.asInstanceOf[BufferedIterator[RowReader]].head.getDouble(1)
//        }(ordering)
        buf

      }.map(Observable.fromIterable)

      Observable.fromTask(resultRv).flatten
    } else {
      source
    }
  }
  override def funcParams: Seq[FuncArgs] = Nil
}

final case class ScalarFunctionMapper(function: ScalarFunctionId,
                                      timeStepParams: RangeParams) extends RangeVectorTransformer {
  protected[exec] def args: String = s"function=$function, funcParams=$funcParams"

  def scalarImpl(source: Observable[RangeVector]): Observable[RangeVector] = {

    val resultRv = source.toListL.map { rvs =>
      if (rvs.size > 1) {
        Seq(ScalarFixedDouble(timeStepParams, Double.NaN))
      } else {
        Seq(ScalarVaryingDouble(rvs.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toMap))
      }
    }.map(Observable.fromIterable)
    Observable.fromTask(resultRv).flatten
  }
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    function match {
      case Scalar => scalarImpl(source)
      case _ => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
  override def funcParams: Seq[FuncArgs] = Nil

}

final case class VectorFunctionMapper() extends RangeVectorTransformer {
  protected[exec] def args: String = s"funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    source.map { rv =>
      new RangeVector {
        override def key: RangeVectorKey = rv.key
        override def rows: Iterator[RowReader] = rv.rows
      }
    }
  }
  override def funcParams: Seq[FuncArgs] = Nil
}

final case class AbsentFunctionMapper(columnFilter: Seq[ColumnFilter], rangeParams: RangeParams, metricColumn: String)
  extends RangeVectorTransformer {

  protected[exec] def args: String =
    s"columnFilter=$columnFilter rangeParams=$rangeParams metricColumn=$metricColumn"

  def keysFromFilter : RangeVectorKey = {
    val labelsFromFilter = columnFilter.filter(_.filter.isInstanceOf[Equals]).
      filterNot(_.column.equals(metricColumn)).map { c =>
      ZeroCopyUTF8String(c.column) -> ZeroCopyUTF8String(c.filter.valuesStrings.head.asInstanceOf[String])
    }.toMap
    CustomRangeVectorKey(labelsFromFilter)
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    def addNonNanTimestamps(res: List[Long], cur: RangeVector): List[Long]  = {
      res ++ cur.rows.filter(!_.getDouble(1).isNaN).map(_.getLong(0)).toList
    }
    val nonNanTimestamps = source.foldLeftL(List[Long]())(addNonNanTimestamps)

    val resultRv = nonNanTimestamps.map {
      t =>
        val rowList = new ListBuffer[TransientRow]()
        for (i <- rangeParams.startSecs to rangeParams.endSecs by rangeParams.stepSecs) {
          if (!t.contains(i * 1000))
            rowList += new TransientRow(i * 1000, 1)
        }
        new RangeVector {
          override def key: RangeVectorKey = if (rowList.isEmpty) CustomRangeVectorKey.empty else keysFromFilter
          override def rows: Iterator[RowReader] = rowList.iterator
          }
        }

    Observable.fromTask(resultRv)
  }
  override def funcParams: Seq[FuncArgs] = Nil
  override def schema(source: ResultSchema): ResultSchema = ResultSchema(Seq(ColumnInfo("timestamp",
    ColumnType.TimestampColumn), ColumnInfo("value", ColumnType.DoubleColumn)), 1)

  override def canHandleEmptySchemas: Boolean = true
}

final case class BucketValues(schema: HistogramBuckets,
                              buckets: debox.Buffer[Array[Double]])

/**
 * Expands a Histogram RV to the equivalent Prometheus
 * data model where we have one vector per histogram bucket.  Changes:
 * - Each bucket converts to a vector with metric name appended with _bucket and extra le= tag
 *
 * If the histogram schema changes, there will be output vectors for each different bucket across all schemas.
 * To simplify conversion, all output vectors will have the same set of timestamps.  If there is no bucket value
 * for that timestamp (due to schema changes), then a NaN will be added.
 *
 * Uses up memory for each bucket of each histogram vector, one vector at a time.  Unfortunately this is memory
 * intensive.  Due to the semantics of Observables and the lazy nature, we cannot reuse data structures as
 * vectors may be pulled by the next reader at the same time this observable processes future vectors.
 */
final case class HistToPromSeriesMapper(sch: PartitionSchema) extends RangeVectorTransformer {
  import RangeVectorTransformer._

  protected[exec] def args: String = s"HistToPromSeriesMapper(options=${sch.options})"

  def funcParams: Seq[FuncArgs] = Nil

  // NOTE: apply() is only called for explicit instantiation of conversion function.  So this will error out if
  //       the data source is not histogram.
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    source.flatMap { rv =>
      Observable.fromIterable(expandVector(rv))
    }
  }

  def expandVector(rv: RangeVector): Seq[RangeVector] = {
    // Data structures to hold bucket values for each possible bucket.
    val timestamps = debox.Buffer.empty[Long]
    val buckets    = debox.Map.empty[Double, debox.Buffer[Double]]
    var curScheme: HistogramBuckets = HistogramBuckets.emptyBuckets
    var emptyBuckets = debox.Set.empty[Double]

    rv.rows.foreach { row =>
      val hist = row.getHistogram(1).asInstanceOf[HistogramWithBuckets]

      if (hist.buckets != curScheme) {
        addNewBuckets(hist.buckets, buckets, timestamps.length)  // add new buckets, backfilling timestamps with NaN
        curScheme = hist.buckets
        emptyBuckets = buckets.keysSet -- curScheme.bucketSet   // All the buckets that need NaN filled going forward
      }

      timestamps += row.getLong(0)
      for { b <- 0 until hist.numBuckets optimized } {
        buckets(hist.bucketTop(b)) += hist.bucketValue(b)
      }
      emptyBuckets.foreach { b => buckets(b) += Double.NaN }
    }

    // Now create new RangeVectors for each bucket
    // NOTE: debox.Map methods sometimes has issues giving consistent results instead of duplicates.
    buckets.mapToArray { case (le, bucketValues) =>
      promBucketRV(rv.key, le, timestamps, bucketValues)
    }.toSeq
  }

  override def schema(source: ResultSchema): ResultSchema =
    if (valueColumnType(source) != ColumnType.HistogramColumn) source else
    ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)), 1)

  private def addNewBuckets(newScheme: HistogramBuckets,
                            buckets: debox.Map[Double, debox.Buffer[Double]],
                            elemsToPad: Int): Unit = {
    val bucketsToAdd = newScheme.bucketSet -- buckets.keysSet
    bucketsToAdd.foreach { buc =>
      buckets(buc) = debox.Buffer.fill(elemsToPad)(Double.NaN)
    }
  }

  val LeUtf8 = ZeroCopyUTF8String("le")

  // Create a Prometheus-compatible single bucket range vector
  private def promBucketRV(origKey: RangeVectorKey, le: Double,
                           ts: debox.Buffer[Long], values: debox.Buffer[Double]): RangeVector = {
    // create new range vector key, appending _bucket to the metric name
    val labels2 = origKey.labelValues.map { case (k, v) =>
      if (k == sch.options.metricUTF8) (k, ZeroCopyUTF8String(v.toString + "_bucket")) else (k, v)
    }
    val lab3 = labels2.updated(LeUtf8, ZeroCopyUTF8String(if (le == Double.PositiveInfinity) "+Inf" else le.toString))
    BufferRangeVector(CustomRangeVectorKey(lab3, origKey.sourceShards), ts, values)
  }
}

