package filodb.core.downsample

import java.lang.{Double => JLDouble}

import debox.Buffer
import enumeratum.{Enum, EnumEntry}
import scalaxy.loops._

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.DataSchema
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.memory.format.{vectors => bv, PrimitiveVectorReader}
import filodb.memory.format.vectors.{CorrectingDoubleVectorReader, DoubleVectorDataReader}
/**
  * Enum of supported downsampling function names
  * @param entryName name of the function
  * @param downsamplerClass its corresponding ChunkDownsampler class used for instance construction
  */
sealed abstract class DownsamplerName (override val entryName: String, val downsamplerClass: Class[_])
  extends EnumEntry

object DownsamplerName extends Enum[DownsamplerName] {
  val values = findValues
  case object MinD extends DownsamplerName("dMin", classOf[MinDownsampler])
  case object MaxD extends DownsamplerName("dMax", classOf[MaxDownsampler])
  case object SumD extends DownsamplerName("dSum", classOf[SumDownsampler])
  case object SumH extends DownsamplerName("hSum", classOf[HistSumDownsampler])
  case object CountD extends DownsamplerName("dCount", classOf[CountDownsampler])
  case object AvgD extends DownsamplerName("dAvg", classOf[AvgDownsampler])
  case object AvgAcD extends DownsamplerName("dAvgAc", classOf[AvgAcDownsampler])
  case object AvgScD extends DownsamplerName("dAvgSc", classOf[AvgScDownsampler])
  case object TimeT extends DownsamplerName("tTime", classOf[TimeDownsampler])
  case object LastD extends DownsamplerName("dLast", classOf[LastValueDDownsampler])
  case object LastH extends DownsamplerName("hLast", classOf[LastValueHDownsampler])
}

sealed abstract class PeriodMarkerName(override val entryName: String, val periodMarkerClass: Class[_])
  extends EnumEntry

object PeriodMarkerName extends Enum[PeriodMarkerName] {
  val values = findValues
  case object Default extends PeriodMarkerName("def", classOf[DefaultDownsamplePeriodMarker])
  case object Counter extends PeriodMarkerName("hLast", classOf[CounterDownsamplePeriodMarker])
}

/**
  * Common trait for implementations of a chunk downsampler
  */
trait ChunkDownsampler {

  /**
    * Ids of Data Columns the downsampler works on.
    * The column id values are fed in via downsampling configuration of the dataset
    */
  def inputColIds: Seq[Int]

  /**
    * Downsampler name
    */
  def name: DownsamplerName

  /**
    * Type of the downsampled value emitted by the downsampler.
    */
  def outputColType: ColumnType

  /**
    * String representation of the downsampler for human readability and string encoding.
    */
  def encoded: String = s"${name.entryName}(${inputColIds.mkString("@")})"
}


trait DownsamplePeriodMarker {

  def name: PeriodMarkerName

  /**
    * Ids of Data Columns the marker works on.
    * The column id values are fed in via downsampling configuration of the dataset
    */
  def inputColIds: Seq[Int]

  /**
    * Places row numbers for the given chunkset which marks the
    * periods to downsample into the result buffer param
    */
  def getPeriods(part: ReadablePartition,
                 chunkset: ChunkSetInfoReader,
                 resMillis: Long,
                 startRow: Int,
                 endRow: Int): Buffer[Int]
}

class DefaultDownsamplePeriodMarker(val inputColIds: Seq[Int]) extends DownsamplePeriodMarker {
  require(inputColIds == Nil)
  // scalastyle:off parameter.number
  override def getPeriods(part: ReadablePartition,
                          chunkset: ChunkSetInfoReader,
                          resMillis: Long,
                          startRow: Int,
                          endRow: Int): Buffer[Int] = {

    val tsAcc = chunkset.vectorAccessor(DataSchema.timestampColID)
    val tsPtr = chunkset.vectorAddress(DataSchema.timestampColID)
    val tsReader = part.chunkReader(DataSchema.timestampColID, tsAcc, tsPtr).asLongReader

    val startTime = tsReader.apply(tsAcc, tsPtr, startRow)
    val endTime = tsReader.apply(tsAcc, tsPtr, endRow)

    val result = debox.Buffer.empty[Int]
    // A sample exactly for 5pm downsampled 5-minutely should fall in the period 4:55:00:001pm to 5:00:00:000pm.
    // Hence subtract - 1 below from chunk startTime to find the first downsample period.
    // + 1 is needed since the startTime is inclusive. We don't want pStart to be 4:55:00:000;
    // instead we want 4:55:00:001
    var pStart = ((startTime - 1) / resMillis) * resMillis + 1
    var pEnd = pStart + resMillis // end is inclusive
    // for each downsample period
    while (pStart <= endTime) {
      // fix the boundary row numbers for the downsample period by looking up the timestamp column
      val endRowNum = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, pEnd), chunkset.numRows - 1)
      result += endRowNum
      pStart += resMillis
      pEnd += resMillis
    }
    result
  }
  override def name: PeriodMarkerName = PeriodMarkerName.Default
}

class CounterDownsamplePeriodMarker(inputColIds: Seq[Int]) extends DefaultDownsamplePeriodMarker(inputColIds) {
  require(inputColIds.length == 1)
  override def getPeriods(part: ReadablePartition,
                          chunkset: ChunkSetInfoReader,
                          resMillis: Long,
                          startRow: Int,
                          endRow: Int): Buffer[Int] = {
    val result = debox.Set.empty[Int]
    result += startRow // need to add start of every chunk
    result ++= super.getPeriods(part, chunkset, resMillis, startRow + 1, endRow)
    val ctrVecAcc = chunkset.vectorAccessor(inputColIds.head)
    val ctrVecPtr = chunkset.vectorAddress(inputColIds.head)
    val ctrReader = part.chunkReader(inputColIds.head, ctrVecAcc, ctrVecPtr)
    ctrReader match {
      case r: DoubleVectorDataReader =>
        if (PrimitiveVectorReader.dropped(ctrVecAcc, ctrVecPtr)) { // counter dip detected
          val drops = r.asInstanceOf[CorrectingDoubleVectorReader].dropPositions(ctrVecAcc, ctrVecPtr)
          for {i <- 0 until drops.length optimized} {
            result += drops(i - 1)
            result += drops(i)
          }
        }
      case _ =>
        throw new IllegalStateException("Did not get a double column - cannot apply counter period marking strategy")
    }
    import spire.std.int._
    result.toSortedBuffer
  }
}

/**
  * Chunk downsampler that emits Double values
  */
trait DoubleChunkDownsampler extends ChunkDownsampler {
  override val outputColType: ColumnType = ColumnType.DoubleColumn

  /**
    * Downsamples Chunk using column Ids configured and emit double value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Double
}

/**
  * Chunk downsampler trait for downsampling timestamp columns; emits long timestamps
  */
trait TimeChunkDownsampler extends ChunkDownsampler {
  override val outputColType: ColumnType = ColumnType.TimestampColumn

  /**
    * Downsamples Chunk using timestamp column Ids configured and emit long value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Long
}

/**
 * Chunk downsampler trait for downsampling histogram columns -> histogram columns
 */
trait HistChunkDownsampler extends ChunkDownsampler {
  override val outputColType: ColumnType = ColumnType.HistogramColumn

  /**
    * Downsamples Chunk using histogram column Ids configured and emit histogram value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): bv.Histogram
}

/**
  * Downsamples by calculating sum of values in one column
  */
case class SumDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"Sum downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.SumD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr)
    colReader.asDoubleReader.sum(vecAcc, vecPtr, startRow, endRow)
  }
}

case class HistSumDownsampler(val inputColIds: Seq[Int]) extends HistChunkDownsampler {
  require(inputColIds.length == 1, s"Sum downsample requires only one column. Got ${inputColIds.length}")
  val name = DownsamplerName.SumH
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): bv.Histogram = {
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val histReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr).asHistReader
    histReader.sum(startRow, endRow)
  }
}

/**
  * Downsamples by calculating count of values in one column
  */
case class CountDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"Count downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.CountD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr)
    colReader.asDoubleReader.count(vecAcc, vecPtr, startRow, endRow)
  }
}

/**
  * Downsamples by calculating min of values in one column
  */
case class MinDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"Min downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.MinD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    // TODO MinOverTimeChunkedFunctionD has same code.  There is scope for refactoring logic into the vector class.
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr)
    var min = Double.MaxValue
    var rowNum = startRow
    val it = colReader.iterate(vecAcc, vecPtr, startRow).asDoubleIt
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) min = Math.min(min, nextVal)
      rowNum += 1
    }
    min
  }
}


/**
  * Downsamples by calculating min of values in one column
  */
case class LastValueDDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"LastValue downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.LastD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr).asDoubleReader
    colReader.apply(vecAcc, vecPtr, endRow)
  }
}

/**
  * Downsamples by calculating min of values in one column
  */
case class LastValueHDownsampler(override val inputColIds: Seq[Int]) extends HistChunkDownsampler {
  require(inputColIds.length == 1, s"LastValue downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.LastH
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): bv.Histogram = {
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr).asHistReader
    colReader.apply(endRow)
  }
}

/**
  * Downsamples by calculating max of values in one column
  */
case class MaxDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"Max downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.MaxD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    // TODO MaxOverTimeChunkedFunctionD has same code.  There is scope for refactoring logic into the vector class.
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr)
    var max = Double.MinValue
    var rowNum = startRow
    val it = colReader.iterate(vecAcc, vecPtr, startRow).asDoubleIt
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) max = Math.max(max, nextVal)
      rowNum += 1
    }
    max
  }
}

/**
  * Downsamples by calculating average from average and count columns
  */
case class AvgAcDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 2, s"AvgAc downsample requires column ids of avg and count. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgAcD
  val avgCol = inputColIds(0)
  val countCol = inputColIds(1)
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val avgVecAcc = chunkset.vectorAccessor(avgCol)
    val avgVecPtr = chunkset.vectorAddress(avgCol)
    val avgColReader = part.chunkReader(avgCol, avgVecAcc, avgVecPtr)
    val cntVecAcc = chunkset.vectorAccessor(countCol)
    val cntVecPtr = chunkset.vectorAddress(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecAcc, cntVecPtr)
    var rowNum = startRow
    val avgIt = avgColReader.iterate(avgVecAcc, avgVecPtr, startRow).asDoubleIt
    val cntIt = cntColReader.iterate(cntVecAcc, cntVecPtr, startRow).asDoubleIt
    var avg = 0d
    var cnt = 0d
    while (rowNum <= endRow) {
      val nextAvg = avgIt.next
      val nextCnt = cntIt.next
      avg = (avg * cnt + nextAvg * nextCnt) / (nextCnt + cnt)
      cnt = cnt + nextCnt
      rowNum += 1
    }
    avg
  }
}

/**
  * Downsamples by calculating average from sum and count columns
  */
case class AvgScDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 2, s"AvgSc downsample requires column ids of sum and count. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgScD
  val sumCol = inputColIds(0)
  val countCol = inputColIds(1)
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val sumVecAcc = chunkset.vectorAccessor(sumCol)
    val sumVecPtr = chunkset.vectorAddress(sumCol)
    val sumColReader = part.chunkReader(sumCol, sumVecAcc, sumVecPtr)
    val cntVecAcc = chunkset.vectorAccessor(countCol)
    val cntVecPtr = chunkset.vectorAddress(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecAcc, cntVecPtr)
    val sumSum = sumColReader.asDoubleReader.sum(sumVecAcc, sumVecPtr, startRow, endRow)
    val sumCount = cntColReader.asDoubleReader.sum(cntVecAcc, cntVecPtr, startRow, endRow)
    sumSum / sumCount
  }
}

/**
  * Downsamples by calculating average of values from one column
  */
case class AvgDownsampler(override val inputColIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(inputColIds.length == 1, s"Avg downsample requires one column id with " +
    s"data to average. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr)
    val sum = colReader.asDoubleReader.sum(vecAcc, vecPtr, startRow, endRow)
    val count = colReader.asDoubleReader.count(vecAcc, vecPtr, startRow, endRow)
    sum / count
  }
}

/**
  * Downsamples by selecting the last timestamp in the downsample period.
  */
case class TimeDownsampler(override val inputColIds: Seq[Int]) extends TimeChunkDownsampler {
  require(inputColIds.length == 1, s"Time downsample requires only one column. Got ${inputColIds.length}")
  override val name: DownsamplerName = DownsamplerName.TimeT
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Long = {
    val vecPtr = chunkset.vectorAddress(inputColIds(0))
    val vecAcc = chunkset.vectorAccessor(inputColIds(0))
    val colReader = part.chunkReader(inputColIds(0), vecAcc, vecPtr).asLongReader
    colReader.apply(vecAcc, vecPtr, endRow)
  }
}

object ChunkDownsampler {

  /**
    * Parses single downsampler from string notation such as
    * "dAvgAc(4@1)" where "dAvgAc" is the downsampler name, 4 & 1 are the column IDs to be used by the function
    */
  def downsampler(strNotation: String): ChunkDownsampler = {
    val parts = strNotation.split("[(@)]")
    // TODO possibly better validation of string notation
    require(parts.size >= 2, s"Downsampler '$strNotation' does not have downsampler name and column id. ")
    val name = parts(0)
    val colIds = parts.drop(1).map(_.toInt)
    DownsamplerName.withNameOption(name) match {
      case None    => throw new IllegalArgumentException(s"Unsupported downsampling function $name")
      case Some(d) => d.downsamplerClass.getConstructor(classOf[Seq[Int]])
                            .newInstance(colIds.toSeq).asInstanceOf[ChunkDownsampler]
    }
  }

  def downsamplers(str: Seq[String]): Seq[ChunkDownsampler] = str.map(downsampler(_))
}

object DownsamplePeriodMarker {

  /**
    * Parses single downsampler from string notation such as
    * "counter(2)" where "counter" is the downsample period marker name, 2 is the column IDs to be used by the function
    */
  def downsamplePeriodMarker(strNotation: String): DownsamplePeriodMarker = {
    val parts = strNotation.split("[()]")
    // TODO possibly better validation of string notation
    require(parts.nonEmpty, s"DownsamplePeriodMarker '$strNotation' needs a name")
    val name = parts(0)
    val colIds = parts.drop(1).map(_.toInt)
    PeriodMarkerName.withNameOption(name) match {
      case None    => throw new IllegalArgumentException(s"Unsupported downsampling function $name")
      case Some(d) => d.periodMarkerClass.getConstructor(classOf[Seq[Int]])
        .newInstance(colIds.toSeq).asInstanceOf[DownsamplePeriodMarker]
    }
  }

  val defaultDownsamplePeriodMarker = new DefaultDownsamplePeriodMarker(Nil)
}
