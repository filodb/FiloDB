#!/usr/bin/env bash

if [[ $# -ne 4 ]]; then
    echo "Usage: $0 <FILO_ADMIN_KEYSPACE> <FILO_KEYSPACE> <DATASET> <NUM_SHARDS>"
    echo "First Run: $0 filodb_admin filodb prometheus 4 > ddl.cql"
    echo "Then Run: cql --request-timeout 3000 -f ddl.cql"
    exit 1
fi

FILO_ADMIN_KEYSPACE=$1
FILO_KEYSPACE=$2
DATASET=$3
NUM_SHARDS=$4

cat << EOF
TRUNCATE ${FILO_ADMIN_KEYSPACE}.checkpoints;
TRUNCATE ${FILO_KEYSPACE}.${DATASET}_tschunks;
TRUNCATE ${FILO_KEYSPACE}.${DATASET}_ingestion_time_index;
EOF

for SHARD in $(seq 0 $((NUM_SHARDS - 1))); do

cat << EOF
TRUNCATE ${FILO_KEYSPACE}.${DATASET}_partitionkeys_$SHARD;
EOF

done
