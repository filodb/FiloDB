#!/usr/bin/env bash
set -euo pipefail

# Runs a local Prometheus with the remote-write receiver enabled, then runs the
# FiloDB-vs-Prometheus compatibility spec against it.

PROM_IMAGE="prom/prometheus:v2.38.0"
PROM_CONTAINER="filodb-promcompat"
PROM_PORT="${PROM_PORT:-9090}"
export PROM_URL="http://localhost:${PROM_PORT}"

cleanup() { docker rm -f "${PROM_CONTAINER}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

cleanup
echo "Starting Prometheus (${PROM_IMAGE}) on :${PROM_PORT} ..."
# NOTE: no out-of-order window flag. The generator writes samples in ascending
# time order per series into a freshly started Prometheus, so they land in the
# head block and need no out-of-order support. (The CLI flag
# --storage.tsdb.out-of-order-time-window does not exist in Prometheus 2.38.0.)
docker run -d --name "${PROM_CONTAINER}" -p "${PROM_PORT}:9090" "${PROM_IMAGE}" \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.enable-remote-write-receiver >/dev/null

echo "Waiting for Prometheus readiness ..."
for _ in $(seq 1 30); do
  if curl -sf "${PROM_URL}/-/ready" >/dev/null; then break; fi
  sleep 1
done
curl -sf "${PROM_URL}/-/ready" >/dev/null || { echo "Prometheus not ready"; exit 1; }

echo "Running the Prometheus compatibility harness (PROM_URL=${PROM_URL}) ..."
# The harness lives in http/src/it (the sbt IntegrationTest config). `it:test` runs the
# whole harness: the PromCompatSpec (which uses this Prometheus) plus the support-class
# unit specs (DataGenSpec, QuerySetSpec, ResultComparatorSpec, PromRemoteClientSpec).
sbt "http/it:test"
