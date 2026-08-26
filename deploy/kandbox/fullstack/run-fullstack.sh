#!/usr/bin/env bash
# Full mosaic-local integ-test stack on one Kandbox pod.
# Default FiloDB topology: main cluster (2 nodes) + p2 (1) + p3 (1), 4 shards each.
# Uses start-mosaic's own subcommands only (baked configs) -> no custom conf,
# no FiloServer/kafka launch command in this process' argv, so the running-guards
# do not false-match.

set -u
BASE=/opt/mosaic-local
cd "$BASE"

# JAVA_TOOL_OPTIONS applies to every JVM that does NOT set the same flag itself:
#  -Xmx2g caps Query Service x3, Gateway x4, Traffic Router, Metro, Zookeeper
#         (FiloDB -Xmx4G, Cassandra, Kafka pass their own -Xmx and win).
#  -Dfilodb.http.bind-host=0.0.0.0 makes the FiloServer HTTP listeners bind all
#         interfaces instead of localhost. REQUIRED: the k8s liveness/readiness
#         probes and the Kandbox public ingress reach the pod IP, not 127.0.0.1.
#         start-mosaic's startFiloDB does not set this, so we inject it here.
#         (Harmless / ignored by the non-FiloDB JVMs.)
export JAVA_TOOL_OPTIONS="-Xmx2g -Dfilodb.http.bind-host=0.0.0.0"

# gRPC remote Query Service mode (matches integ-test.sh). Combined with the -g
# flag on startQueryService below, QS uses mosaic-local-grpc.conf and talks to
# FiloDB as a remote gRPC client instead of embedding a cluster actor system,
# which avoids the fatal akka 'provider=cluster' conflict.
export ENABLE_GRPC_REMOTE_QS=true

log() { echo "[fullstack] $*"; }

# run <timeout-seconds> <start-mosaic-subcommand...>
# Each start-mosaic start* has an internal health-wait loop; some loop with no max
# retry. Wrap in `timeout` so one stuck service cannot wedge the whole sequence.
run() {
  local t="$1"; shift
  log ">>> $* (timeout ${t}s)"
  if timeout "$t" ./start-mosaic.sh "$@"; then
    log "<<< $* OK"
  else
    log "<<< WARNING: '$*' returned non-zero or timed out; continuing"
  fi
}

# 1) Infrastructure
run 300 startCassandra
run 90  startZookeeper
run 180 startKafka

# 2) FiloDB FIRST - default topology (main x2 + p2 + p3, 4 shards each).
#    Brought up early so :8080 is listening well before the liveness deadline and
#    the pod can go Ready while the slower services below still start.
run 420 startFiloDB

# 3) Gateway - binary variant (4 instances: main + sssp + p2 + p3)
run 240 startGatewayBinary

# 4) Traffic Router + partitions. MUST precede Query Service: the QS health check
#    curls /api/v1/query which routes through Traffic Router; if TR is down the
#    check never returns HTTP 200 and start-mosaic aborts QS startup.
run 180 startTrafficRouter
run 150 createTrafficRouterPartitions

# 5) Query Service is NOT baked into the image -> download it, then start it.
QS_OK=0
log "setup: downloading Query Service binary (setupBinaryQueryService)..."
if timeout 600 ./start-mosaic.sh setupBinaryQueryService; then
  QS_OK=1
  log "Query Service binary downloaded OK"
else
  log "WARNING: Query Service download FAILED. Query FiloDB directly on :8080."
fi
if [ "$QS_OK" = "1" ]; then
  # -g => gRPC remote mode (mosaic-local-grpc.conf). See integ-test-start.sh.
  run 300 -g startQueryService
else
  log "skip startQueryService (binary missing)"
fi

# 6) Metro - synthetic metric generator (drives the ingest_n_query data path)
run 180 startMetroBinary

# 7) Prometheus (optional self-metrics scrape)
run 120 startPrometheus

# 8) Kick off a Metro ingest job so there is data to query. Full 28-field JobConfig
#    (the baked scripts/ingest_n_query payload is stale for this Metro build).
#    Generates Counter1/Gauge1/Histogram1/Summary1 under
#    _ws_="aci-telemetry", _ns_="filodb-local-0" via gateway ingest port 9898.
log "submitting Metro ingest job (synthetic metrics, 120 min)..."
if curl -sS -m 30 -X POST http://localhost:8090/api/v1/jobs \
    -H 'Content-Type: application/json' \
    -d '{
      "name": "LocalMetroJob",
      "minutesToRun": 120,
      "jobConfig": {
        "numApplications": 1,
        "applicationPrefix": "filodb-local",
        "tagKeyPrefix": ["tag1", "tag2", "tag3"],
        "tagValueCounts": [ 2, 5, 10 ],
        "numInstances": 3,
        "numCountersPerInstance": 1,
        "numGaugesPerInstance": 1,
        "numHistogramsPerInstance": 1,
        "numSummariesPerInstance": 1,
        "numDeltaCountersPerInstance": 0,
        "numDeltaHistogramsPerInstance": 0,
        "numOtelDeltaHistogramsPerInstance": 0,
        "numOtelDeltaExpHistogramsPerInstance": 0,
        "expBucketsPerHistogram": 0,
        "bucketsPerHistogram": 20,
        "quantilesPerSummary": 5,
        "instanceLifetimeMinutes": 7200,
        "tickIntervalSeconds": 10,
        "promIngestionEndpoint": "",
        "mosaicIngestionServerTLSPeerHost": "mosaic.aci-telemetry-perf.kube",
        "mosaicIngestionServerHost": "localhost",
        "mosaicIngestionServerPort": 9898,
        "mosaicIngestionEndpointDiscovery": false,
        "mosaicAuthPublishEnabled": false,
        "mosaicWorkspace": "aci-telemetry",
        "mosaicGrpcClientsCount": 1,
        "dispatcher": "mosaic",
        "ingestionClusterTypeString": "mosaic"
      }
    }'; then
  echo; log "Metro ingest job submitted"
else
  log "WARNING: Metro ingest job submit failed (Metro not up?)"
fi

echo
log "=== startup sequence complete; running JVMs: ==="
ps -ef | grep -E 'FiloServer|query-server|mosaic-metrics-gateway|mosaic-traffic-router|metro.Main|CassandraDaemon|QuorumPeerMain' | grep -v grep || true
echo
log "Query endpoints (bound 0.0.0.0):"
log "  Query Service (public):  :9900   (Prometheus-compatible)"
log "  FiloDB direct/status:    :8080/api/v1/cluster/prometheus/status"
log "  Metro jobs:              :8090/api/v1/jobs"
echo
log "keep-alive: holding pid 1 open with 'tail -f /dev/null'"
exec tail -f /dev/null
