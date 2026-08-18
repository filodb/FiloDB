# FiloDB in-sandbox smoke test on Kandbox — design

- Date: 2026-08-18
- Status: Approved (design). Next step: implementation plan.
- Owner: Siri Varma Vegiraju

## Goal

Prove that FiloDB starts and runs healthy inside a Kandbox cloud sandbox. The run
carries **8 shards on one FiloDB node** with a **~6 GB off-heap block-memory
budget**. This is a functional smoke test, not a load test.

The original request named "32 pods, 8 shards, 6 GB data per pod". A smoke test
does not need 32 pods (~192 GB). It needs one healthy node. So the design keeps
the 8 shards and the ~6 GB budget, and drops the pod count to 1.

## Scope

In scope:
- One Kandbox cloud sandbox.
- One pod that runs Cassandra, Kafka, and a single FiloDB `FiloServer` (plus the
  query service), all inside the sandbox.
- Config overrides to set 8 shards, ~6 GB block memory, and an 8-partition Kafka
  topic.

Out of scope:
- 32 pods / ~192 GB.
- Filling 6 GB with real data. The 6 GB is an allocated budget, not a fill target.
- Managed Cassandra / Kafka (see "Alternatives considered").
- The production `filodb-timeseries` image.

## Key facts (verified)

- **Kandbox** is an ephemeral-environment service. A cloud sandbox is a namespace
  in a multi-node Kubernetes cluster. It pulls images only from Apple Artifactory
  (`docker.apple.com`). A `DeploymentConfig` YAML describes the apps.
- **FiloDB is not standalone.** It needs Cassandra (persistence), Kafka
  (ingestion), and an Akka cluster seed.
- **`mosaic-query-tools` integration tests use their own Kafka + Cassandra.** They
  run real Kafka, Zookeeper, and Cassandra binaries as OS processes, started by
  `start-mosaic.sh` from the `mosaic-local-docker` repo. In CI, the whole stack is
  baked into one image: `docker.apple.com/telemetry/mosaic-local:latest`. FiloDB
  runs as embedded `filodb.standalone.FiloServer` JVMs wired to `localhost`.
- **FiloDB config meaning:**
  - `num-shards` is per dataset in the source conf. The one-box default is 4. Set
    it to 8.
  - `filodb.min-num-nodes-in-cluster` controls how shards spread across nodes. Set
    it to 1 so one node owns all 8 shards.
  - "6 GB data per pod" maps to the off-heap block-memory budget, which is the sum
    of `store.shard-mem-size` across the shards on that node. It is **not** the JVM
    `-Xmx` and **not** the container memory limit.
  - `num-shards` must equal the Kafka topic partition count. So the topic needs 8
    partitions.

## Approach

Deploy `docker.apple.com/telemetry/mosaic-local:latest` as one Kandbox app. This
is the exact stack the integration tests use. FiloDB in this image already talks
to localhost Kafka/Cassandra through OSS providers, so no config fight.

### The Kandbox app

- Name: `filodb-onebox`. Type: `deployment`. `replicas: 1`.
- Command: run `start-mosaic.sh` and start only these services:
  - Cassandra
  - Kafka (with its bundled Zookeeper)
  - one FiloDB `FiloServer`
  - the query service (so we can run a query)
  - Skip the p2/p3 FiloDB clusters, gateway, metro, and Prometheus to keep the pod
    lean.
- Config overrides, mounted with Kandbox `files` or `configMaps` over the image's
  `conf/filodb/*`:
  - `timeseries-dev-source.conf`: `num-shards = 8`,
    `sourceconfig.store.shard-mem-size ≈ 768MB` (8 × 768 MB ≈ 6 GB). Keep
    `bootstrap.servers = "localhost:9092"`.
  - server conf: `filodb.min-num-nodes-in-cluster = 1`, single-node cluster
    discovery and a single Akka seed that points at the same node.
  - `FiloServer` JVM: `-Xmx` ≈ 2–4 G.
  - Kafka topic `timeseries-dev`: create with **8 partitions** (override the topic
    creation in `start-mosaic.sh`).
- Resources:
  - requests: ~4 cpu / 12 Gi
  - limits: ~6 cpu / **16 Gi** (Cassandra + Kafka + 2–4 G heap + 6 G block memory
    + query service)
- Ports: query service `9900` (main), FiloDB http `8080`.
- Probes: readiness on FiloDB shard status or query-service health, with a long
  `failureThreshold` because FiloDB needs several minutes to become Ready. Forward
  the FiloDB file log (`logsDirectory`) for diagnosis, since FiloDB writes real
  logs to a file, not stdout.

## Success criteria

1. The pod is Ready. Cassandra, Kafka, and `FiloServer` are up. No crashes.
   FiloDB reports `started=true`.
2. The Kafka topic `timeseries-dev` has 8 partitions. Cassandra keyspaces exist.
3. FiloDB reports **8 shards ACTIVE**.
4. The FiloDB startup log confirms a ~6 GB block-memory budget.
5. Optional: push a few samples and read them back through the query service. This
   confirms the ingest-to-query path.

## Prerequisites (user owns)

- Install the `kandbox` CLI: `brew install apple/amp-cloud/kandbox`.
- Install the Kandbox Claude Code plugin, then restart Claude Code:
  - `/plugin marketplace add git@github.pie.apple.com:ASE-DevX/devx-skills.git`
  - `/plugin install kandbox@devx-skills`
- DCVPN active. AppleConnect Narrative settings on.
- Access Manager "Kandbox" access. Done (manager approved).

After install, Claude drives create and deploy through the plugin's MCP.

## Open items to resolve during planning

Cheap subagents (Haiku or Sonnet) will confirm these before any deploy, so the
plan has no guesses:

1. The `mosaic-local:latest` image entrypoint. Does it need a custom
   `command`/`args`, or does it auto-run `start-mosaic.sh`?
2. The exact `start-mosaic.sh` command to start only Cassandra, Kafka, one
   FiloServer, and the query service.
3. The exact file paths and contents for the config overrides
   (`timeseries-dev-source.conf`, server conf) inside the image.
4. Where `start-mosaic.sh` creates the `timeseries-dev` topic, and how to set 8
   partitions.
5. How the image forces a multi-node FiloDB cluster, so the override can force a
   single node.

## Risks

- Cloud-sandbox quota may reject a single ~16 Gi pod. Mitigation: lower
  `shard-mem-size` if needed, or split memory.
- The one-box may hardcode a multi-node FiloDB cluster. Mitigation: the override
  forces `min-num-nodes-in-cluster = 1` and a single seed.
- The image entrypoint may not accept a partial-start command cleanly. Mitigation:
  planning item 1 and 2 confirm this first.

## Alternatives considered

- **Managed Cassandra@Apple + Kafka@Apple.** Closest to production, but it needs
  decrypted Cassandra credentials and a shared Kaffe mTLS cert mounted as secrets,
  a dedicated 8-partition Kaffe topic provisioned out of band, and FiloDB/infra
  sign-off because it rides shared dev clusters. Too heavy for a smoke test.
  Revisit for a later production-like run.
- **Production `filodb-timeseries` image with in-sandbox Cassandra/Kafka.** The
  image is wired for Kaffe and Cassandra discovery. Using it with plain localhost
  services means overriding those providers back to OSS defaults. More config
  fight than value here.
- **Separate Cassandra + Kafka + FiloDB apps, hand-built.** More YAML and more
  wiring than the one-box, with no added benefit for a smoke test.
