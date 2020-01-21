#!/usr/bin/env bash

if [[ $# -ne 6 ]]; then
    echo "Usage: $0 <FILO_ADMIN_KEYSPACE> <FILO_KEYSPACE> <FILO_DOWNSAMPLE_KEYSPACE> <DATASET> <NUM_SHARDS> <RESOLUTIONS>"
    echo "First Run:    $0 filodb_admin filodb filodb_downsample prometheus 4 1,5 > ddl.cql"
    echo "Then Run:     cql -u cass_username -p cass_password --request-timeout 3000 -f ddl.cql cass_host 9992"
    echo "Or for Local: cql --request-timeout 3000 -f ddl.cql"
    exit 1
fi

FILO_ADMIN_KEYSPACE=$1
FILO_KEYSPACE=$2
FILO_DOWNSAMPLE_KEYSPACE=$3
DATASET=$4
NUM_SHARDS=$5
IFS=',' read -r -a RESOLUTIONS <<< "$6"

function truncate_chunk_tables() {

local KEYSP=$1
local DSET=$2

cat << EOF
TRUNCATE ${KEYSP}.${DSET}_tschunks;
TRUNCATE ${KEYSP}.${DSET}_ingestion_time_index;
EOF
}

function truncate_partkey_tables() {

local KEYSP=$1
local DSET=$2

for SHARD in $(seq 0 $((NUM_SHARDS - 1))); do
cat << EOF
TRUNCATE ${KEYSP}.${DSET}_partitionkeys_$SHARD;
EOF
done

cat << EOF
TRUNCATE ${KEYSP}.${DSET}_partitionkeys_by_update_time
EOF

}

function truncate_admin_tables() {

cat << EOF
TRUNCATE ${FILO_ADMIN_KEYSPACE}.checkpoints;
EOF

}

truncate_admin_tables

truncate_chunk_tables ${FILO_KEYSPACE} ${DATASET}
truncate_partkey_tables ${FILO_KEYSPACE} ${DATASET}

for RES in "${RESOLUTIONS[@]}"
do
    truncate_chunk_tables ${FILO_DOWNSAMPLE_KEYSPACE} "${DATASET}_ds_${RES}"
done

truncate_partkey_tables ${FILO_DOWNSAMPLE_KEYSPACE} "${DATASET}_ds_${RESOLUTIONS[-1]}"
