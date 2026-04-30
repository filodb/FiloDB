package filodb.core.memstore.aggregation

/**
 * Groups the schema-level aggregation configuration: which columns to aggregate, at what interval,
 * and how much out-of-order tolerance to allow.
 */
case class SchemaAggregationConfig(aggregators: Seq[ColumnAggregator] = Seq.empty,
                                   intervalMs: Long = 0L,
                                   oooToleranceMs: Long = 0L) {
  def nonEmpty: Boolean = aggregators.nonEmpty
}

object SchemaAggregationConfig {
  val empty = SchemaAggregationConfig()
}

/**
 * Defines a schema-level aggregator for a specific column, parsed from a downsampler-style
 * string notation like "dSum(1)" or "hSum(3)".
 *
 * @param columnId the data column index this aggregator applies to (must be > 0, since 0 is timestamp)
 * @param aggType the type of aggregation to perform
 */
case class ColumnAggregator(columnId: Int, aggType: AggregationType)

object ColumnAggregator {

  private val nameToAggType: Map[String, AggregationType] = Map(
    "dSum"   -> AggregationType.Sum,
    "dMin"   -> AggregationType.Min,
    "dMax"   -> AggregationType.Max,
    "dLast"  -> AggregationType.Last,
    "dFirst" -> AggregationType.First,
    "dAvg"   -> AggregationType.Avg,
    "dCount" -> AggregationType.Count,
    "hSum"   -> AggregationType.HistogramSum,
    "hLast"  -> AggregationType.HistogramLast
  )

  /**
   * Parses a single aggregator from string notation such as "dSum(1)" where "dSum" is the
   * aggregator name and 1 is the column ID.
   *
   * Uses the same `[(@)]` split as `ChunkDownsampler.downsampler`.
   */
  def parse(strNotation: String): ColumnAggregator = {
    val parts = strNotation.split("[(@)]")
    require(parts.length >= 2, s"Aggregator '$strNotation' must have name and column id")
    val name = parts(0)
    val colId = parts(1).toInt
    val aggType = nameToAggType.getOrElse(name,
      throw new IllegalArgumentException(s"Unknown aggregator: $name"))
    ColumnAggregator(colId, aggType)
  }

  def parseAll(strs: Seq[String]): Seq[ColumnAggregator] = strs.map(parse)
}
