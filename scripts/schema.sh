#!/usr/bin/env bash

if [[ $# -ne 4 ]]; then
    echo "Usage: $0 <FILO_ADMIN_KEYSPACE> <FILO_KEYSPACE> <DATASET> <NUM_SHARDS>"
    echo "First Run: $0 filodb_admin filodb prometheus 4 > ddl.cql"
    echo "Then Run: cql -f ddl.cql"
    exit 1
fi

FILO_ADMIN_KEYSPACE=$1
FILO_KEYSPACE=$2
DATASET=$3
NUM_SHARDS=$4

cat << EOF
CREATE TABLE  IF NOT EXISTS ${FILO_ADMIN_KEYSPACE}.checkpoints (
    databasename text,
    datasetname text,
    shardnum int,
    groupnum int,
    offset bigint,
    PRIMARY KEY ((databasename, datasetname, shardnum), groupnum)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

EOF

cat << EOF
CREATE TABLE  IF NOT EXISTS ${FILO_KEYSPACE}.${DATASET}_tschunks (
    partition blob,
    chunkid bigint,
    chunks frozen<list<blob>>,
    info blob,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF

cat << EOF
CREATE TABLE IF NOT EXISTS ${FILO_KEYSPACE}.${DATASET}_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF

for SHARD in $(seq 0 $((NUM_SHARDS - 1))); do

cat << EOF
CREATE TABLE IF NOT EXISTS ${FILO_KEYSPACE}.${DATASET}_partitionkeys_$SHARD (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF

done
