<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Lucene Indexing](#lucene-indexing)
  - [Time Bucket Creation during Normal Ingestion](#time-bucket-creation-during-normal-ingestion)
  - [Index Recovery](#index-recovery)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Lucene Indexing

A Lucene index keeps track of startTime, endTime, tags and full partKey for every time series ingested within
retention period. It is used during the first part of query processing when relevant time series are identified
for a given query filter. 

The timestamp of the first sample of the time series is used as startTime and the last sample is used as endTime for
indexing. For actively ingesting partitions, endTime is Long.MaxValue.

A document in the Lucene index is upserted whenever a partKey (time series) starts/stops/restarts ingesting.
Besides updating the local Lucene index, this data persisted to Cassandra for recovery on failure.
The persist operations happen as part of the flush task. When a node restarts, records from Cassandra
are first scanned to re-populate the local Lucene index before any ingestion starts.

## Cassandra Schema

PartKeys, start/endTimes are stored in a cassandra table with partKey as the partition/primary key.
For efficient scans of entire shard's worth of data, we have a table per shard. Token scans are employed
to read data quickly while bootstrapping FiloDB shard.
 
## Index Recovery from Cassandra

One of the first steps when ingestion starts on a node is index recovery. We download the data from cassandra
and repopulate the Lucene index.

For time series that are ingesting (endTime == Long.MaxValue) TimeSeriesPartition objects are created on heap. For
those that have stopped ingesting, we do not load them into heap. It is done lazily when a query asks for it.  

## Start / End Time Detection and Flush

A list per shard tracks dirty partKeys that need persistence to cassandra. Part Key data is persisted in one randomly
designated flush group per shard. This is to scatter the writes across shards and reduce cassandra peak write load. 
During flush we also identify any partition that is marked as ingesting but has no data to be flushed as a
non-ingesting partition and add them to the dirty keys that need to be persisted.

## Kafka Checkpointing

We piggy-back on regular flush group checkpointing as follows:

For new partitions and restart-ingestion events, flushing of key data happens during the randomly designated group for
the shard. Thus, checkpointing of this event will never be behind the last flush group's checkpoint. During kafka
recovery, the partKey is added to index and marked as dirty for persistence even if we skip the sample.

End-of-ingestion detection is done along with chunk flush, and persistence of end of time series marker will always
be consistent with its group checkpoint. If for any reason node fails before persisting end-of-ingestion, the partition
will be recovered as actively ingesting, and will eventually be marked as non-ingesting during its flush.
