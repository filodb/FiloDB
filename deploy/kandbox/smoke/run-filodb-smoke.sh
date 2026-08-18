#!/usr/bin/env bash
# Start Cassandra + Kafka + a single 8-shard FiloDB node inside the mosaic-local image.
set -euo pipefail

BASE=/opt/mosaic-local
SMOKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE"

echo "[smoke] starting Cassandra (also creates FiloDB keyspaces)..."
./start-mosaic.sh startCassandra

echo "[smoke] starting Zookeeper..."
./start-mosaic.sh startZookeeper

echo "[smoke] starting Kafka..."
./start-mosaic.sh startKafka

echo "[smoke] locating kafka-topics.sh..."
KAFKA_TOPICS="$(find "$BASE/thirdparty" -name kafka-topics.sh -type f 2>/dev/null | head -1)"
if [ -z "$KAFKA_TOPICS" ]; then
  echo "[smoke] ERROR: kafka-topics.sh not found under $BASE/thirdparty" >&2
  exit 1
fi

echo "[smoke] creating 8-partition topic timeseries-smoke..."
"$KAFKA_TOPICS" --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 8 --topic timeseries-smoke --if-not-exists
echo "[smoke] topic description:"
"$KAFKA_TOPICS" --describe --zookeeper localhost:2181 --topic timeseries-smoke

echo "[smoke] launching single FiloServer (8 shards, ~6GB block memory)..."
FILODB_JAR="$BASE/filodb/lib/filodb-pie-assembly.jar"
if [ ! -f "$FILODB_JAR" ]; then
  echo "[smoke] ERROR: FiloDB jar not found at $FILODB_JAR" >&2
  exit 1
fi
mkdir -p "$BASE/logs"

exec java -Xmx2G \
  -Dfilodb.cassandra.part-keys-v2-table-enabled=true \
  -Dconfig.file="$SMOKE_DIR/smoke-server.conf" \
  -Dfilodb.v2-cluster-enabled=true \
  -Dfilodb.cluster-discovery.localhost-ordinal=0 \
  -Dfilodb.http.bind-host=0.0.0.0 \
  -Dfilodb.http.bind-port=8080 \
  -Dlogback.configurationFile="$SMOKE_DIR/smoke-logback.xml" \
  -DlogSuffix=smoke \
  -cp "$FILODB_JAR" \
  filodb.standalone.FiloServer
