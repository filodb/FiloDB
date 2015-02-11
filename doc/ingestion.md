
## Ingestion Goals

1. Don't lose any data!  Idempotent at-least-once
2. Backpressure.  Ingest only when ready.
3. Support distributed bulk ingest
4. Efficient ingest and efficient retries
5. Should work for streaming data, including error recovery

NOTE: The ingestion is designed for high-volume ingestion of fewer datasets at a
time, due to the nature of bulk columnar chunk ingestion.  It is not designed
for lots of tiny row ingestions across a huge number of datasets at once.  It is
advised that for lots of datasets, they be ingested a few at a time.

## Use Case - Append-Only Writes

- User divides dataset into independent partitions, sequences input rows
- FiloDB internally shards partitions and tracks row IDs
- Row IDs numbered from 0 upwards, corresponds to partition input stream
- Row IDs are always increasing, unless rewinding for replay / recovery
- Row ID of each column chunk is the first row # of each chunk
- Append-only pattern: write to shard with highest starting row ID
- Regular acks of incoming stream

## Use Case - Random Writes

- Each row may have a different row ID
- Inefficient - cannot easily group chunks of rows together
- Need to worry about replacing existing data
- Best that the row ingester can do is to group successive row IDs together, and reset when it gets something different - or do a sliding window and group everything in the window together
- Also much harder to shard

## Don't Lose Any Data

- Increasing sequence numbers Kafka-style for each row/chunk of ingress stream
- Ack latest committed sequence number
    + Works for any data layout
    + Scalable, works when grouping rows into columnar layout
- On error:
    + Rewind to last committed sequence number (may rely on client for replay)
    + Updates to all state needs to be like CRDTs - repeatable, idempotent
- Sequence number should be independent of row #
    + For appends, they should both increase at the same rate, such that row # = sequence # + (offset)
    + For random writes, no correlation

## Sharding 

See [[ShardingStrategy.scala]] for specific strategies.

Sharding is needed to limit physical row length. Each row stores columnar chunks of rows.
1. Distributing shards of physical row chunks eliminates hot spots
2. Due to columnar nature of storage, reading out rows requires a transpose operation.  This operation requires more memory the longer each physical row is.

Simplest sharding strategy is fixed hashing by row number.  It works for both random and append writes, less state to keep track of.

Sharding is basically a function.  

(Existing shard state, row IDs) -> (New shard state)

More complex sharding strategy can be based on the actual # of bytes written, and adjust subsequent shard size based on previous input.  This is very tricky to get right though, and makes managing shards esp for random writes very complex.

A middle ground is a EstimatingHashingSharder.  This uses info from the schema to estimate the number of rows per shard to balance shard size and simplicity. However, since the sharding strategy has to be declared when a partition is created, there is potential for divergence if a lot of columns are added in later versions.

## Ingestion API

**High-level Row API** - ingest individual rows with a sequence # and Row ID.  A RowIngester groups the rows into chunks aligned with the chunksize and translates into columnar format.
- Also takes care of replaces by reading older chunks and doing operations on it

**Low-level columnar API** - `Columns(rowId: Long, endingSequenceNo: Long, columns: Map[String, ColumnBuilder[_]])`
- Assumed that starting row IDs are already chunk-aligned
- Only for append patterns, does not handle replacements

## Ingestion walk-through

![](filodb_ingestion_flow.png)

Prerequisites for ingestion:
1. Dataset must be created
2. Columns/schema must be created
3. At least one partition must be created
4. Columns to be ingested must be defined in the schema already
