package filodb.core.query

import java.util.regex.Pattern

import filodb.core.metadata.{Column, DataColumn, Dataset}
import filodb.memory.format.{SingleValueRowReader, UTF8Wrapper, ZeroCopyUTF8String}

sealed trait Filter {
  def filterFunc: Any => Boolean
  def operatorString: String
  def valuesStrings : Set[Any]
}

object Filter {
  final case class Equals(value: Any) extends Filter {
    override def filterFunc: Any => Boolean = (item: Any) => value.equals(item)
    val operatorString: String = "="
    def valuesStrings: Set[Any] = Set(value)
  }

  final case class In(values: Set[Any]) extends Filter {
    override def filterFunc: (Any) => Boolean = (item: Any) => values.contains(item)
    val operatorString: String = "in"
    def valuesStrings: Set[Any] = values
  }

  final case class And(left: Filter, right: Filter) extends Filter {
    private val leftFunc = left.filterFunc
    private val rightFunc = right.filterFunc
    override def filterFunc: (Any) => Boolean = (item: Any) => leftFunc(item) && rightFunc(item)
    val operatorString: String = "&&"
    def valuesStrings: Set[Any] = left.valuesStrings.union(right.valuesStrings)
  }

  final case class NotEquals(value: Any) extends Filter {
    override def filterFunc: (Any) => Boolean = (item: Any) => !value.equals(item)
    val operatorString: String = "!="
    def valuesStrings: Set[Any] = Set(value)
  }

  final case class EqualsRegex(value: Any) extends Filter {
    val pattern = Pattern.compile(value.toString, Pattern.DOTALL)
    override def filterFunc: (Any) => Boolean = (item: Any) =>  pattern.matcher(item.toString).matches()
    val operatorString: String = "=~"
    def valuesStrings: Set[Any] = Set(value)
  }

  final case class NotEqualsRegex(value: Any) extends Filter {
    val pattern = Pattern.compile(value.toString, Pattern.DOTALL)
    override def filterFunc: (Any) => Boolean = (item: Any) =>  !pattern.matcher(item.toString).matches()
    val operatorString: String = "!~"
    def valuesStrings: Set[Any] = Set(value)
  }

}

final case class ColumnFilter(column: String, filter: Filter)

/**
 * Utilities to generate functions to filter keys.
 */
object KeyFilter {
  // Parses the literal in an expression through a KeyType's key function... intended mostly for
  // ComputedColumns so that proper transformation of a value can happen for predicate pushdowns.
  // For example, if a partition column uses :stringPrefix, then apply that first to a value.
  def parseSingleValue(col: Column, value: Any): Any =
    col.extractor.getField(SingleValueRowReader(value), 0) match {
      case z: ZeroCopyUTF8String => UTF8Wrapper(z)
      case o: Any                => o
    }

  def parseValues(col: Column, values: Iterable[Any]): Iterable[Any] =
    values.map(v => parseSingleValue(col, v))

  // Decodes wrapped values for parsing, esp for UTF8
  def decode(value: Any): Any = value match {
    case UTF8Wrapper(z) => z
    case s: String      => ZeroCopyUTF8String(s)
    case o: Any         => o
  }

  /**
   * Identifies column names belonging to a projection's partition key columns and their positions within
   * the partition key.  For computed columns, if there is only one source column, the source column is used.
   * Computed columns with multiple source columns are ignored.
   * NOTE: computed columns not currently supported here; to support them in the future we'd need to update
   * computed columns to properly compute the position
   * @param dataset a Dataset
   * @param columnNames the names of columns to match
   * @return a Map(column name -> (position, Column)) of identified partition columns
   */
  def mapPartitionColumns(dataset: Dataset, columnNames: Seq[String]): Map[String, (Int, Column)] =
    mapColumns(dataset.partitionColumns, columnNames)

  def mapRowKeyColumns(dataset: Dataset, columnNames: Seq[String]): Map[String, (Int, Column)] =
    mapColumns(dataset.rowKeyColumns, columnNames)

  def mapColumns(columns: Seq[Column],
                 columnNames: Seq[String]): Map[String, (Int, Column)] = {
    columns.zipWithIndex.collect {
      case d @ (DataColumn(_, name, _, _), idx)           => name -> (idx -> d._1)
    }.toMap.filterKeys { name => columnNames.contains(name) }
  }

  // NOTE: With Lucene indexing coming, partition filter func is really not needed anymore

  /**
   * Creates a filter function that returns boolean given a PartitionKey.
   * @param dataset the Dataset describing the dataset schema
   * @param filters one ColumnFilter per column to filter on.  If multiple filters are desired on that
   *                column they should be combined using And.
   */
  private def needSomethingHereForDocNotToComplain(): Unit = ???

  // def makePartitionFilterFunc(dataset: Dataset,
  //                             filters: Seq[ColumnFilter]): PartitionKey => Boolean = {
  //   val positionsAndFuncs = filters.map { case ColumnFilter(col, filter) =>
  //                             val pos = dataset.partitionColumns.indexWhere(_.name == col)
  //                             (pos, filter.filterFunc) }
  //   val positions = positionsAndFuncs.collect { case (pos, func) if pos >= 0 => pos }.toArray
  //   val funcs = positionsAndFuncs.collect { case (pos, func) if pos >= 0 => func }.toArray

  //   def partFunc(p: PartitionKey): Boolean = {
  //     cforRange { 0 until positions.size } { i =>
  //       val bool = funcs(i)(p.getAny(positions(i)))
  //       // Short circuit when any filter returns false
  //       if (!bool) return false
  //     }
  //     true
  //   }
  //   partFunc
  // }

  // def makePartitionFilterFunc(dataset: Dataset, filter: ColumnFilter): PartitionKey => Boolean =
  //   makePartitionFilterFunc(dataset, Seq(filter))
}
