# ADR: Aggregating Buffers for Out-of-Order Tolerance

## Status
Accepted

## Context

Currently, FiloDB's `TimeSeriesPartition.ingest` method drops samples that arrive out-of-order (timestamp < last seen timestamp). This is problematic for real-world scenarios where network delays, retries, or distributed systems can cause samples to arrive slightly out of sequence.

### Current Ingestion Flow

1. Read timestamp from row
2. If last seen timestamp > current timestamp → drop as out-of-order
3. If new chunk with appendable buffers needed → create chunk (one buffer per column type)
4. If buffers at boundary (time or space) → switch buffers and seal chunks for persistence

### Requirements

- Support configurable out-of-order tolerance (e.g., up to 1 minute)
- Aggregate samples within time buckets to handle late arrivals
- Configure aggregation type per column in schema (sum, avg, min, max, last, etc.)
- Maintain backward compatibility with existing schemas
- Bound memory usage regardless of out-of-order patterns

## Decision

Implement **AggregatingTimeSeriesPartition** with time-bucketed aggregation buffers.

### Design Overview

#### 1. Time Bucketing Strategy

**Approach**: Ceil timestamps to configurable interval boundaries

```scala
def ceilToBucket(ts: Long, bucketIntervalMs: Long): Long = {
  ((ts + bucketIntervalMs - 1) / bucketIntervalMs) * bucketIntervalMs
}
```

**Example**: With 30-second buckets:
- Sample at 12:00:05 → bucket 12:00:30
- Sample at 12:00:25 → bucket 12:00:30
- Sample at 12:00:31 → bucket 12:01:00

#### 2. Schema Configuration

**Extend column definition syntax** in `filodb-defaults.conf`:

```hocon
schemas {
  aggregating-gauge {
    columns = ["timestamp:ts", "value:double:{aggregation=sum,interval=30s,ooo-tolerance=60s}"]
    ...
  }
}
```

**Parameters**:
- `aggregation`: Type of aggregation (sum, avg, min, max, last, first, count)
- `interval`: Bucketing interval (e.g., 30s, 1m, 5m)
- `ooo-tolerance`: Maximum out-of-order tolerance window

**Parsing**: Extends existing parameter pattern (like `detectDrops=true`, `counter=true`)

#### 3. Architecture Components

##### Core Classes

```
AggregatingTimeSeriesPartition extends TimeSeriesPartition
  ├─ TimeBucket (per interval)
  │   ├─ bucketTimestamp: Long
  │   ├─ aggregators: Array[Aggregator]
  │   └─ sampleCount: Int
  │
  ├─ BucketManager
  │   ├─ activeBuckets: Map[Long, TimeBucket]
  │   ├─ maxBuckets: Int (default 3)
  │   └─ evictOldest(): Unit
  │
  └─ Aggregator trait
      ├─ SumAggregator
      ├─ AvgAggregator
      ├─ MinAggregator
      ├─ MaxAggregator
      ├─ LastAggregator
      ├─ FirstAggregator
      └─ CountAggregator
```

##### Key Data Structures

**TimeBucket**:
```scala
case class TimeBucket(
  bucketTimestamp: Long,
  aggregators: Array[Aggregator],
  var sampleCount: Int = 0
) {
  def aggregate(columnValues: Array[Any]): Unit
  def emit(): Array[Any]  // Returns aggregated values
  def canAccept(ts: Long, interval: Long, tolerance: Long): Boolean
}
```

**Aggregator Trait**:
```scala
trait Aggregator {
  def add(value: Any): Unit
  def result(): Any
  def reset(): Unit
}
```

**AggregationConfig** (per column):
```scala
case class AggregationConfig(
  columnIndex: Int,
  aggType: AggregationType,
  intervalMs: Long,
  oooToleranceMs: Long
)

sealed trait AggregationType
object AggregationType {
  case object Sum extends AggregationType
  case object Avg extends AggregationType
  case object Min extends AggregationType
  case object Max extends AggregationType
  case object Last extends AggregationType
  case object First extends AggregationType
  case object Count extends AggregationType
}
```

#### 4. Ingestion Flow with Aggregation

```
ingest(row) {
  1. Extract timestamp and values from row

  2. For each column with aggregation config:
     a. Calculate bucket timestamp: ceilToBucket(ts, interval)
     b. Get or create TimeBucket for bucket timestamp
     c. Check if bucket can accept sample (within tolerance)
     d. If yes: bucket.aggregate(values)
     e. If no: emit bucket and create new one

  3. Manage bucket lifecycle:
     a. If activeBuckets.size > maxBuckets:
        - Sort by timestamp
        - Emit oldest bucket(s)
        - Remove from activeBuckets

  4. For emitted buckets:
     a. Create aggregated row with bucket timestamp
     b. Call super.ingest(aggregatedRow) to use existing buffer logic
}
```

#### 5. Memory Management

**Bounded Bucket Retention**:
- Maximum 3 active buckets per partition (configurable)
- Buckets: current, previous (for late arrivals), transition buffer
- When limit exceeded → force emit oldest bucket

**Memory Calculation**:
```
Memory per partition =
  maxBuckets × (
    sizeof(Long) +                    // bucket timestamp
    numColumns × sizeof(Aggregator) + // aggregator state
    sizeof(Int)                       // sample count
  )

Example: 3 buckets × (8 + 5 columns × 16 + 4) ≈ 276 bytes per partition
```

#### 6. Integration Points

##### Schema Definition (core/src/main/resources/filodb-defaults.conf)

Add aggregation examples:
```hocon
schemas {
  aggregating-gauge {
    columns = [
      "timestamp:ts",
      "value:double:{aggregation=sum,interval=30s,ooo-tolerance=60s}"
    ]
    partition-columns = ["metric:string", "tags:map"]
    value-column = "value"
  }

  aggregating-multi-column {
    columns = [
      "timestamp:ts",
      "min:double:{aggregation=min,interval=1m,ooo-tolerance=2m}",
      "max:double:{aggregation=max,interval=1m,ooo-tolerance=2m}",
      "sum:double:{aggregation=sum,interval=1m,ooo-tolerance=2m}",
      "count:long:{aggregation=count,interval=1m,ooo-tolerance=2m}"
    ]
    partition-columns = ["metric:string", "tags:map"]
  }
}
```

##### Column Metadata (core/src/main/scala/filodb.core/metadata/Column.scala)

Extend `Column` class to parse and store aggregation parameters:
```scala
case class Column(
  name: String,
  columnType: ColumnType,
  // ... existing fields ...
  aggregationConfig: Option[AggregationConfig] = None
) {
  // Parse from params like "aggregation=sum,interval=30s,ooo-tolerance=60s"
}
```

##### Partition Factory (core/src/main/scala/filodb.core/memstore/TimeSeriesMemStore.scala)

Modify partition creation to detect aggregation config:
```scala
def createPartition(...): TimeSeriesPartition = {
  val hasAggregation = schema.data.columns.exists(_.aggregationConfig.isDefined)

  if (hasAggregation) {
    new AggregatingTimeSeriesPartition(...)
  } else {
    new TimeSeriesPartition(...)
  }
}
```

## Implementation Plan

### Phase 1: Core Aggregation Framework

**Files to Create**:

1. `core/src/main/scala/filodb.core/memstore/aggregation/AggregationType.scala`
   - Define `AggregationType` sealed trait and implementations
   - Define `AggregationConfig` case class

2. `core/src/main/scala/filodb.core/memstore/aggregation/Aggregator.scala`
   - Define `Aggregator` trait
   - Implement: SumAggregator, AvgAggregator, MinAggregator, MaxAggregator
   - Implement: LastAggregator, FirstAggregator, CountAggregator

3. `core/src/main/scala/filodb.core/memstore/aggregation/TimeBucket.scala`
   - Implement `TimeBucket` class
   - Time bucketing logic: `ceilToBucket()`
   - Aggregation per column
   - Emit aggregated values

4. `core/src/main/scala/filodb.core/memstore/aggregation/BucketManager.scala`
   - Manage active buckets map
   - Implement bucket eviction policy
   - Track bucket lifecycle

**Files to Modify**:

5. `core/src/main/scala/filodb.core/metadata/Column.scala`
   - Add `aggregationConfig: Option[AggregationConfig]` field
   - Parse aggregation parameters from column definition string
   - Add helper methods: `hasAggregation`, `getAggregationConfig`

6. `core/src/main/resources/filodb-defaults.conf`
   - Add example aggregating schemas
   - Document aggregation parameter syntax
   - Add configuration for default bucket limits

### Phase 2: Aggregating Partition Implementation

**Files to Create**:

7. `core/src/main/scala/filodb.core/memstore/AggregatingTimeSeriesPartition.scala`
   - Extend `TimeSeriesPartition`
   - Override `ingest()` method
   - Implement bucket-based aggregation logic
   - Delegate to `super.ingest()` for aggregated samples
   - Add metrics for aggregation statistics

**Files to Modify**:

8. `core/src/main/scala/filodb.core/memstore/TimeSeriesMemStore.scala`
   - Modify partition factory method
   - Detect aggregation config in schema
   - Instantiate `AggregatingTimeSeriesPartition` when needed
   - Add configuration for aggregation settings

9. `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`
   - Pass aggregation config to partition creation
   - Add metrics for aggregated vs non-aggregated partitions

### Phase 3: Testing

**Files to Create**:

10. `core/src/test/scala/filodb.core/memstore/aggregation/AggregatorSpec.scala`
    - Unit tests for all aggregator types
    - Edge cases: NaN, infinity, nulls
    - Type conversion tests

11. `core/src/test/scala/filodb.core/memstore/aggregation/TimeBucketSpec.scala`
    - Time bucketing logic tests
    - Boundary conditions
    - Multi-column aggregation

12. `core/src/test/scala/filodb.core/memstore/aggregation/BucketManagerSpec.scala`
    - Bucket eviction tests
    - Memory bounds verification
    - Concurrent access patterns

13. `core/src/test/scala/filodb.core/memstore/AggregatingTimeSeriesPartitionSpec.scala`
    - Integration tests for ingestion flow
    - Out-of-order sample handling
    - Tolerance window tests
    - Comparison with non-aggregating partition

14. `core/src/test/scala/filodb.core/metadata/ColumnSpec.scala` (modify)
    - Add tests for aggregation parameter parsing
    - Invalid configuration handling

### Phase 4: Documentation and Configuration

**Files to Modify**:

15. `doc/ingestion.md`
    - Document aggregating buffers feature
    - Usage examples
    - Configuration guide
    - Performance considerations

16. `README.md`
    - Add aggregating buffers to feature list
    - Link to detailed documentation

## Detailed Implementation Steps

### Step 1: Aggregator Framework (AggregationType.scala, Aggregator.scala)

```scala
// AggregationType.scala
package filodb.core.memstore.aggregation

sealed trait AggregationType
object AggregationType {
  case object Sum extends AggregationType
  case object Avg extends AggregationType
  case object Min extends AggregationType
  case object Max extends AggregationType
  case object Last extends AggregationType
  case object First extends AggregationType
  case object Count extends AggregationType

  def parse(s: String): Option[AggregationType] = s.toLowerCase match {
    case "sum" => Some(Sum)
    case "avg" | "average" => Some(Avg)
    case "min" => Some(Min)
    case "max" => Some(Max)
    case "last" => Some(Last)
    case "first" => Some(First)
    case "count" => Some(Count)
    case _ => None
  }
}

case class AggregationConfig(
  columnIndex: Int,
  aggType: AggregationType,
  intervalMs: Long,
  oooToleranceMs: Long
)
```

```scala
// Aggregator.scala
package filodb.core.memstore.aggregation

trait Aggregator {
  def add(value: Any): Unit
  def result(): Any
  def reset(): Unit
  def copy(): Aggregator
}

class SumAggregator extends Aggregator {
  private var sum: Double = 0.0
  private var count: Int = 0

  def add(value: Any): Unit = {
    value match {
      case d: Double => sum += d; count += 1
      case l: Long => sum += l.toDouble; count += 1
      case i: Int => sum += i.toDouble; count += 1
      case f: Float => sum += f.toDouble; count += 1
      case _ => // ignore non-numeric
    }
  }

  def result(): Any = if (count > 0) sum else Double.NaN
  def reset(): Unit = { sum = 0.0; count = 0 }
  def copy(): Aggregator = new SumAggregator
}

class AvgAggregator extends Aggregator {
  private var sum: Double = 0.0
  private var count: Int = 0

  def add(value: Any): Unit = {
    value match {
      case d: Double => sum += d; count += 1
      case l: Long => sum += l.toDouble; count += 1
      case i: Int => sum += i.toDouble; count += 1
      case f: Float => sum += f.toDouble; count += 1
      case _ => // ignore
    }
  }

  def result(): Any = if (count > 0) sum / count else Double.NaN
  def reset(): Unit = { sum = 0.0; count = 0 }
  def copy(): Aggregator = new AvgAggregator
}

class MinAggregator extends Aggregator {
  private var min: Double = Double.MaxValue
  private var initialized: Boolean = false

  def add(value: Any): Unit = {
    val d = value match {
      case d: Double => d
      case l: Long => l.toDouble
      case i: Int => i.toDouble
      case f: Float => f.toDouble
      case _ => return
    }
    if (!initialized || d < min) {
      min = d
      initialized = true
    }
  }

  def result(): Any = if (initialized) min else Double.NaN
  def reset(): Unit = { min = Double.MaxValue; initialized = false }
  def copy(): Aggregator = new MinAggregator
}

// Similar implementations for MaxAggregator, LastAggregator, FirstAggregator, CountAggregator
```

### Step 2: TimeBucket and BucketManager (TimeBucket.scala, BucketManager.scala)

```scala
// TimeBucket.scala
package filodb.core.memstore.aggregation

case class TimeBucket(
  bucketTimestamp: Long,
  aggregators: Array[Aggregator],
  var sampleCount: Int = 0
) {
  def aggregate(columnValues: Array[Any]): Unit = {
    require(columnValues.length == aggregators.length,
      s"Column count mismatch: ${columnValues.length} vs ${aggregators.length}")

    var i = 0
    while (i < aggregators.length) {
      aggregators(i).add(columnValues(i))
      i += 1
    }
    sampleCount += 1
  }

  def emit(): Array[Any] = {
    val results = new Array[Any](aggregators.length)
    var i = 0
    while (i < aggregators.length) {
      results(i) = aggregators(i).result()
      i += 1
    }
    results
  }

  def canAccept(ts: Long, intervalMs: Long, toleranceMs: Long): Boolean = {
    val bucketStart = bucketTimestamp - intervalMs
    val bucketEnd = bucketTimestamp
    val windowStart = bucketStart - toleranceMs
    val windowEnd = bucketEnd + toleranceMs

    ts >= windowStart && ts <= windowEnd
  }

  def reset(): Unit = {
    aggregators.foreach(_.reset())
    sampleCount = 0
  }
}

object TimeBucket {
  def ceilToBucket(ts: Long, intervalMs: Long): Long = {
    ((ts + intervalMs - 1) / intervalMs) * intervalMs
  }

  def create(bucketTs: Long, aggregatorTemplates: Array[Aggregator]): TimeBucket = {
    val aggregators = aggregatorTemplates.map(_.copy())
    TimeBucket(bucketTs, aggregators, 0)
  }
}
```

```scala
// BucketManager.scala
package filodb.core.memstore.aggregation

import scala.collection.mutable

class BucketManager(
  maxBuckets: Int = 3,
  aggregatorTemplates: Array[Aggregator]
) {
  private val activeBuckets = mutable.Map[Long, TimeBucket]()

  def getOrCreateBucket(bucketTs: Long): TimeBucket = {
    activeBuckets.getOrElseUpdate(bucketTs, {
      evictIfNeeded()
      TimeBucket.create(bucketTs, aggregatorTemplates)
    })
  }

  def getBucket(bucketTs: Long): Option[TimeBucket] = {
    activeBuckets.get(bucketTs)
  }

  private def evictIfNeeded(): Unit = {
    if (activeBuckets.size >= maxBuckets) {
      // Evict oldest bucket
      val oldestTs = activeBuckets.keys.min
      activeBuckets.remove(oldestTs)
    }
  }

  def evictBucketsOlderThan(thresholdTs: Long): Seq[(Long, TimeBucket)] = {
    val toEvict = activeBuckets.filter { case (ts, _) => ts < thresholdTs }.toSeq
    toEvict.foreach { case (ts, _) => activeBuckets.remove(ts) }
    toEvict
  }

  def allBuckets: Seq[(Long, TimeBucket)] = activeBuckets.toSeq.sortBy(_._1)

  def size: Int = activeBuckets.size

  def clear(): Unit = activeBuckets.clear()
}
```

### Step 3: Column Metadata Extension (Column.scala modifications)

```scala
// In core/src/main/scala/filodb.core/metadata/Column.scala

case class Column(
  name: String,
  columnType: ColumnType,
  // ... existing fields ...
  aggregationConfig: Option[AggregationConfig] = None
) {

  def hasAggregation: Boolean = aggregationConfig.isDefined

  def getAggregationConfig: AggregationConfig =
    aggregationConfig.getOrElse(
      throw new IllegalStateException(s"Column $name has no aggregation config")
    )
}

object Column {
  // Extend existing parsing logic to extract aggregation parameters
  def parseAggregationConfig(params: Map[String, String], columnIndex: Int): Option[AggregationConfig] = {
    for {
      aggTypeStr <- params.get("aggregation")
      aggType <- AggregationType.parse(aggTypeStr)
      intervalStr <- params.get("interval")
      intervalMs = parseTimeString(intervalStr)
      toleranceStr <- params.get("ooo-tolerance")
      toleranceMs = parseTimeString(toleranceStr)
    } yield AggregationConfig(columnIndex, aggType, intervalMs, toleranceMs)
  }

  private def parseTimeString(s: String): Long = {
    // Parse "30s", "1m", "5m", etc. to milliseconds
    val pattern = """(\d+)([smhd])""".r
    s match {
      case pattern(value, unit) =>
        val v = value.toLong
        unit match {
          case "s" => v * 1000
          case "m" => v * 60 * 1000
          case "h" => v * 60 * 60 * 1000
          case "d" => v * 24 * 60 * 60 * 1000
        }
      case _ => throw new IllegalArgumentException(s"Invalid time string: $s")
    }
  }
}
```

### Step 4: AggregatingTimeSeriesPartition (AggregatingTimeSeriesPartition.scala)

```scala
// core/src/main/scala/filodb.core/memstore/AggregatingTimeSeriesPartition.scala

package filodb.core.memstore

import filodb.core.memstore.aggregation._
import filodb.core.metadata.Schema
import filodb.memory.format.UnsafeUtils

class AggregatingTimeSeriesPartition(
  partitionID: Int,
  schema: Schema,
  // ... other parameters same as TimeSeriesPartition ...
) extends TimeSeriesPartition(partitionID, schema, ...) {

  // Extract aggregation configs from schema
  private val aggConfigs: Array[Option[AggregationConfig]] = {
    schema.data.columns.zipWithIndex.map { case (col, idx) =>
      col.aggregationConfig.map(_.copy(columnIndex = idx))
    }.toArray
  }

  // Create aggregator templates
  private val aggregatorTemplates: Array[Aggregator] = {
    aggConfigs.map {
      case Some(config) => createAggregator(config.aggType)
      case None => null // No aggregation for this column
    }
  }

  // Bucket managers per column (only for columns with aggregation)
  private val bucketManagers: Array[BucketManager] = {
    aggConfigs.map {
      case Some(config) =>
        new BucketManager(maxBuckets = 3, Array(aggregatorTemplates(config.columnIndex)))
      case None => null
    }
  }

  private def createAggregator(aggType: AggregationType): Aggregator = aggType match {
    case AggregationType.Sum => new SumAggregator
    case AggregationType.Avg => new AvgAggregator
    case AggregationType.Min => new MinAggregator
    case AggregationType.Max => new MaxAggregator
    case AggregationType.Last => new LastAggregator
    case AggregationType.First => new FirstAggregator
    case AggregationType.Count => new CountAggregator
  }

  override def ingest(
    timestamp: Long,
    row: BinaryRecord,
    offset: Long,
    // ... other parameters ...
  ): Int = {

    // Check if any column has aggregation
    val hasAnyAggregation = aggConfigs.exists(_.isDefined)

    if (!hasAnyAggregation) {
      // No aggregation configured, use standard ingestion
      return super.ingest(timestamp, row, offset, ...)
    }

    // Extract values from row for all columns
    val columnValues = extractColumnValues(row)

    // Process each column with aggregation
    var aggregatedTimestamp = timestamp
    val aggregatedRow = columnValues.clone()

    aggConfigs.zipWithIndex.foreach { case (configOpt, colIdx) =>
      configOpt match {
        case Some(config) =>
          // Calculate bucket timestamp
          val bucketTs = TimeBucket.ceilToBucket(timestamp, config.intervalMs)
          aggregatedTimestamp = bucketTs

          // Get or create bucket
          val bucketManager = bucketManagers(colIdx)
          val bucket = bucketManager.getOrCreateBucket(bucketTs)

          // Check if bucket can accept this sample
          if (bucket.canAccept(timestamp, config.intervalMs, config.oooToleranceMs)) {
            // Aggregate into bucket
            bucket.aggregate(Array(columnValues(colIdx)))

            // For now, don't emit yet - we'll emit on bucket boundary
            // Return early to avoid duplicate ingestion
            return 0 // Indicate sample was aggregated but not yet persisted
          } else {
            // Sample outside tolerance window, emit bucket and create new one
            val emittedValues = bucket.emit()
            aggregatedRow(colIdx) = emittedValues(0)
            bucket.reset()
          }

        case None =>
          // No aggregation for this column, keep original value
      }
    }

    // Check if we should emit buckets (time boundary crossed)
    emitReadyBuckets()

    // For aggregated samples, we defer ingestion until bucket is emitted
    0
  }

  private def emitReadyBuckets(): Unit = {
    // Emit buckets that are ready (based on time or bucket count threshold)
    // This would be called periodically or when certain conditions are met

    aggConfigs.zipWithIndex.foreach { case (configOpt, colIdx) =>
      configOpt match {
        case Some(config) =>
          val bucketManager = bucketManagers(colIdx)

          // Emit buckets older than current time - tolerance
          val currentTime = System.currentTimeMillis()
          val thresholdTs = TimeBucket.ceilToBucket(
            currentTime - config.oooToleranceMs,
            config.intervalMs
          )

          val bucketsToEmit = bucketManager.evictBucketsOlderThan(thresholdTs)

          bucketsToEmit.foreach { case (bucketTs, bucket) =>
            if (bucket.sampleCount > 0) {
              // Create aggregated row and ingest via super class
              val aggregatedValues = bucket.emit()
              val aggregatedRow = createAggregatedRow(bucketTs, aggregatedValues, colIdx)
              super.ingest(bucketTs, aggregatedRow, ...)
            }
          }

        case None => // No aggregation
      }
    }
  }

  private def extractColumnValues(row: BinaryRecord): Array[Any] = {
    // Extract all column values from BinaryRecord
    // Implementation depends on BinaryRecord structure
    ???
  }

  private def createAggregatedRow(
    timestamp: Long,
    aggregatedValues: Array[Any],
    columnIndex: Int
  ): BinaryRecord = {
    // Create BinaryRecord with aggregated values
    // Implementation depends on BinaryRecord structure
    ???
  }
}
```

## Trade-offs and Considerations

### Pros
✅ **Out-of-order tolerance**: Handles late-arriving samples gracefully
✅ **Configurable**: Per-column aggregation types and intervals
✅ **Memory bounded**: Fixed maximum number of buckets prevents unbounded growth
✅ **Backward compatible**: Existing schemas unchanged
✅ **Flexible**: Supports multiple aggregation types (sum, avg, min, max, etc.)

### Cons
❌ **Added complexity**: More complex ingestion path
❌ **Memory overhead**: Additional memory per partition for bucket management
❌ **Latency**: Samples delayed until bucket emission
❌ **Loss of precision**: Original timestamps lost (ceiled to bucket boundary)

### Performance Implications

**Memory**:
- Per partition: ~276 bytes for 3 buckets × 5 columns
- For 1M partitions: ~263 MB additional memory

**CPU**:
- Bucket lookup: O(1) hash map access
- Aggregation: O(1) per sample
- Eviction: O(k) where k = number of buckets (bounded to 3)

**Latency**:
- Samples buffered until bucket emission
- Maximum delay = bucket interval + tolerance window
- Example: 30s interval + 60s tolerance = up to 90s delay

## Metrics and Observability

Add metrics to track:
- `aggregation_buckets_active`: Number of active buckets per partition
- `aggregation_buckets_emitted`: Counter of emitted buckets
- `aggregation_samples_aggregated`: Counter of samples aggregated
- `aggregation_samples_dropped_ooo`: Counter of samples outside tolerance
- `aggregation_bucket_emit_latency`: Histogram of bucket emission times

## Testing Strategy

1. **Unit Tests**: Each aggregator type with edge cases
2. **Integration Tests**: Full ingestion flow with aggregation
3. **Tolerance Tests**: Samples within/outside tolerance window
4. **Memory Tests**: Verify bounded bucket growth
5. **Performance Tests**: Measure overhead of aggregation
6. **Compatibility Tests**: Ensure non-aggregating schemas unchanged

## Future Enhancements

1. **Dynamic bucket intervals**: Adjust interval based on traffic patterns
2. **Spill to disk**: For very large tolerance windows
3. **Cross-partition aggregation**: Aggregate across multiple partitions
4. **Watermarking**: Explicit watermarks for bucket emission
5. **Exactly-once semantics**: Guarantee no duplicate aggregation on replay

## Implementation Notes

Key differences from the original proposal:

1. **Schema-level aggregation config**: Aggregation is configured at the schema level
   (via `AggregationConfig` on the `Schema` object), not per-column as originally proposed.
   All aggregating columns in a schema share the same interval and tolerance.

2. **No maxBuckets cap**: The implementation does not enforce a fixed maximum number of
   active buckets. Buckets are managed by `BucketAggregationState` and flushed based on
   time progression rather than a hard count limit.

3. **Histogram aggregation support**: The implementation includes `HistogramSum` and
   `HistogramLast` aggregator types (via `HistogramAggregator`) that were not in the
   original proposal. These handle `BinaryHistogram` / `HistogramWithBuckets` values.

4. **RowReader-based approach**: Instead of directly manipulating `BinaryRecord` values,
   the implementation uses `RowReader` abstractions (`BucketDataRowReader`,
   `BucketRowData`) to emit aggregated data through the query path via
   `MergingRangeVectorCursor`.

5. **BucketAggregationState**: Replaced the proposed `BucketManager` class. Manages
   bucket lifecycle, sample aggregation, and emission of finalized bucket rows sorted
   by timestamp.

## References

- Current code: `core/src/main/scala/filodb.core/memstore/TimeSeriesPartition.scala`
- Schema definitions: `core/src/main/resources/filodb-defaults.conf`
- Column metadata: `core/src/main/scala/filodb.core/metadata/Column.scala`
