# FiloDB PromQL vs Prometheus compatibility harness

The harness ingests one generated dataset into FiloDB (via CSV) and a live
Prometheus (via remote-write), runs the same PromQL against both over the
Prometheus HTTP API, and asserts the results match within per-query tolerances.

## Run it

    scripts/run_prom_compat.sh

The script starts `prom/prometheus:v2.38.0` in Docker with the remote-write
receiver enabled, then runs `PromCompatSpec`. It removes the container on exit.

To run against an existing Prometheus (remote-write receiver enabled), set the
URL and run the tagged test directly:

    PROM_URL=http://localhost:9090 \
      sbt "http/testOnly filodb.http.promcompat.PromCompatSpec -- -n filodb.http.PrometheusIntegration"

## What is where

- Data generator: `http/src/test/scala/filodb/http/promcompat/DataGen.scala`
- Query sets (HOCON, per category, 3-tuple): `http/src/test/resources/promcompat/queries-*.conf`
- Comparator (tolerant, label-set match): `http/src/test/scala/filodb/http/promcompat/ResultComparator.scala`
- CSV tags support (shared): `coordinator/src/main/scala/filodb.coordinator/sources/CsvStream.scala`

## Scope

Version 1 covers `gauge` and `counter` metric types and the PromQL surface
that applies to them. Histograms are deferred. Multi-partition routing
categories (target-schema, cross-partition, failover) are out of scope.

## Notes

- The launch script needs a working `docker`. It runs
  `prom/prometheus:v2.38.0` with the remote-write receiver enabled and removes
  the container on exit. To run against an already-running Prometheus instead,
  set `PROM_URL` and run the tagged test directly (see "Run it" above).

- The test binds an in-process Akka remoting socket. Run it from a normal
  terminal; sandboxed shells that forbid binding listening sockets will
  abort the suite with `SocketException: Operation not permitted`.

- Each run needs a fresh Prometheus. The generator writes a fixed set of
  series over a window ending ~1 minute in the past; a reused Prometheus that
  already holds those series rejects the overlapping samples as out-of-order
  (HTTP 400). `scripts/run_prom_compat.sh` recreates the container each run.

## Adding queries

Edit `queries-instant.conf` / `queries-range.conf`. Each category is a flat list
of `"promql", "errorLimit", "flags"` triples. `errorLimit`: `""` exact,
`"0.001"` absolute, `"0.1%"` percent.
