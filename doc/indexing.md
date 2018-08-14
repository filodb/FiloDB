<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Lucene Indexing](#lucene-indexing)
  - [Time Bucket Creation during NormalIngestion](#time-bucket-creation-during-normalingestion)
  - [Index Recovery](#index-recovery)
  - [Recovery Behavior during Different Shard States](#recovery-behavior-during-different-shard-states)
    - [Recovering State](#recovering-state)
    - [onIndexBootstrapped State Transition](#onindexbootstrapped-state-transition)
    - [NormalIngestion State](#normalingestion-state)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Lucene Indexing

We use a Lucene index to track startTime, endTime, tags and full partKey for every time series ingested within
retention period. The Lucene index is kept up to date whenever time series starts/stops ingesting.

Besides updating the local Lucene index, these events are added to index time-buckets and persisted for recovery on failure.
The persist operations happen as part of the flush task. When a node restarts, records in the index
time buckets are processed to recover/populate the local Lucene index. The index time bucket recovery
and kafka data recovery streams are merged to form the recovery stream.

## Time Bucket Creation during NormalIngestion

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
* Remove from Lucene partKeys that have stopped ingesting for > retentionPeriod

## Index Recovery

Kafka recovery triggers the creation of TSPartition whereas Index time-bucket recovery does not. Creation
of TSPartition for historical partKeys is done lazily at query time.

During recovery, multiple partIds are not assigned to the same partKey by maintaining a PartitionSet of
`EmptyPartition` objects. This list is cleared and not used after recovery.

```
-----KafkaIngestionRecovery----
                               |
                               +----onIndexBoostrapped------>-------NormalIngestion------>
                               |
-----LoadIndexTimeBuckets------
```

## Recovery Behavior during Different Shard States

### Recovering State

On New PartKey from Index Time-Bucket:
* GetOrAdd partId from `partKeyToPartIdDuringRecovery` partitionSet 
* Upsert partKey to Lucene index. Upsert is needed to override previous entries in index for that partKey from index time buckets.

On New PartKey from IngestData:
* GetOrAdd partId from `partKeyToPartIdDuringRecovery` partitionSet
* Do not add partKey to Lucene index, instead add to `ingestedPartIdsToIndexAfterBootstrap` bitmap. It will be added later.

### onIndexBootstrapped State Transition

* Flush Lucene Index. This is a one-time blocking call.
* For each partition in `ingestedPartIdsToIndexAfterBootstrap`, add key to index if not already in index
* Clear `partKeyToPartIdDuringRecovery` set.
* Start Index flush thread

### NormalIngestion State
 
On New PartKey from IngestData:
* Add partKey to Lucene index immediately
