#!/usr/bin/env bash

if [[ $# -ne 6 ]]; then
    echo "Usage: $0 <FILO_ADMIN_KEYSPACE> <FILO_KEYSPACE> <FILO_DOWNSAMPLE_KEYSPACE> <DATASET> <NUM_SHARDS> <RESOLUTIONS>"
    echo "First Run: $0 filodb_admin filodb filodb_downsample prometheus 4 1,5 > ddl.cql"
    echo "Then Run: cql --request-timeout 3000 -f ddl.cql"
    exit 1
fi

FILO_ADMIN_KEYSPACE=$1
FILO_KEYSPACE=$2
FILO_DOWNSAMPLE_KEYSPACE=$3
DATASET=$4
NUM_SHARDS=$5
IFS=',' read -r -a RESOLUTIONS <<< "$6"


function create_keyspaces() {

local KEYSP=$1

cat << EOF
CREATE KEYSPACE IF NOT EXISTS ${KEYSP} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
EOF

}

function create_chunk_tables() {

local KEYSP=$1
local DSET=$2

cat << EOF
CREATE TABLE IF NOT EXISTS ${KEYSP}.${DSET}_tschunks (
    partition blob,
    chunkid bigint,
    chunks frozen<list<blob>>,
    info blob,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF

cat << EOF
CREATE TABLE IF NOT EXISTS ${KEYSP}.${DSET}_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF
}

function create_partkey_tables() {
local KEYSP=$1
local DSET=$2

for SHARD in $(seq 0 $((NUM_SHARDS - 1))); do

cat << EOF
CREATE TABLE IF NOT EXISTS ${KEYSP}.${DSET}_partitionkeys_$SHARD (
    partKey blob,
    startTime bigint,
    endTime bigint,
    PRIMARY KEY (partKey)
) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};
EOF

done
}

function create_admin_tables() {

cat << EOF
CREATE TABLE IF NOT EXISTS ${FILO_ADMIN_KEYSPACE}.checkpoints (
    databasename text,
    datasetname text,
    shardnum int,
    groupnum int,
    offset bigint,
    PRIMARY KEY ((databasename, datasetname, shardnum), groupnum)
) WITH compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};

EOF

}

create_keyspaces ${FILO_ADMIN_KEYSPACE}
create_keyspaces ${FILO_KEYSPACE}
create_keyspaces ${FILO_DOWNSAMPLE_KEYSPACE}

create_admin_tables

create_chunk_tables ${FILO_KEYSPACE} ${DATASET}
create_partkey_tables ${FILO_KEYSPACE} ${DATASET}

for RES in "${RESOLUTIONS[@]}"
do
    create_chunk_tables ${FILO_DOWNSAMPLE_KEYSPACE} "${DATASET}_ds_${RES}"
done

create_partkey_tables ${FILO_DOWNSAMPLE_KEYSPACE} "${DATASET}_ds_${RESOLUTIONS[-1]}"

