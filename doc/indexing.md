<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Lucene Indexing](#lucene-indexing)
  - [Time Bucket Creation during Normal Ingestion](#time-bucket-creation-during-normal-ingestion)
  - [Index Recovery](#index-recovery)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Lucene Indexing

We use a Lucene index to track startTime, endTime, tags and full partKey for every time series ingested within
retention period. The Lucene index is kept up to date whenever time series starts/stops ingesting.

Besides updating the local Lucene index, these events are added to index time-buckets and persisted for recovery on failure.
The persist operations happen as part of the flush task. When a node restarts, records in the index
time buckets are processed to recover/populate the local Lucene index. The index time bucket recovery
and kafka data recovery streams are merged to form the recovery stream.

## Time Bucket Creation during Normal Ingestion

Index time-buckets are persisted for each shard in random flush groups decided on startup. A time-bucket is one 
flush interval long. Depending on retention period, last N time buckets are important for recovery.

N = ceil(RetentionPeriod / FlushPeriod)

When a new time-bucket is flushed, we also look at the keys N time-buckets ago and bring in the partKeys that are still 
ingesting into the latest time-bucket. This is to ensure that last N time buckets have all the partKeys to be recovered.

We maintain a bitmap per time bucket within every shard. Newly ingested partKeys are added into Lucene index
immediately, and added to the bitmap of latest time bucket. 

During the flush task designated for index time-bucket persistence, following steps are done:
* Bitmap for newest time-bucket is initialized
* Bitmap for latest time-bucket is prepared for flush. This bitmap contains partIds which started/stopped ingesting in the last flush interval
* Look at earliest time bucket and add to time bucket being flushed the partKeys that are still ingesting
* Drop earliest time bucket
* Remove from Lucene partKeys that have stopped ingesting for > retentionPeriod. At the same time, remove the partitions from the shard data structures. 
 
## Index Recovery

One of the first steps when ingestion starts on a node is index recovery. We download the data from cassandra by first
reading the highest time bucket persisted in checkpoints table and fetching the top time buckets that cover retention
time. The buckets are processed in chronological  order.

Each record in the time buckets contain the partKey, startTime and endTime. The entries are upserted into local Lucene
index. While the entries are added to Lucene, TimeSeriesPartition objects are also added to the shard data structures
for each key in the index.

Once all buckets are extracted and re-indexed, we commit the index and start the Kafka recovery stream.
Normal ingestion continues as usual after Kafka recovery.
