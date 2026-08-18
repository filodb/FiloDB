# FiloDB in-sandbox smoke test on Kandbox — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a single-node FiloDB with 8 shards and a ~6 GB off-heap block-memory budget inside a Kandbox cloud sandbox, backed by in-sandbox Cassandra + Kafka, and confirm all 8 shards reach `ShardStatusActive`.

**Architecture:** Deploy the `docker.apple.com/telemetry/mosaic-local:latest` one-box image as a single Kandbox `deployment`. The image bundles Cassandra 4.1.9, Kafka 2.8.2, and the FiloDB assembly jar. A mounted startup script starts Cassandra, Kafka, and Zookeeper through the image's own `start-mosaic.sh`, creates an 8-partition Kafka topic, then launches **one** `filodb.standalone.FiloServer` in the foreground using mounted single-node config overrides (8 shards, 768 MB block memory per shard). Verification hits the FiloDB HTTP API on port 8080.

**Tech Stack:** Kandbox (`DeploymentConfig` YAML + `kandbox` CLI / Kandbox Claude Code plugin), Docker image on Apple Artifactory, FiloDB (HOCON config, Akka), Cassandra, Kafka, Bash.

## Global Constraints

Every task's requirements include these, copied verbatim from the spec and the code extractions:

- Kandbox pulls container images only from Apple Artifactory. Image: `docker.apple.com/telemetry/mosaic-local:latest`.
- In the image: base dir is `/opt/mosaic-local`; runtime user is `observer`; entrypoint is `["/usr/local/bin/tini", "--"]`; `CMD` is `/bin/bash` (the container idles at a shell unless given a command). Cassandra 4.1.9, Kafka 2.8.2, and `/opt/mosaic-local/filodb/lib/filodb-pie-assembly.jar` (FiloDB `develop.0.9.1830`) are baked in. The query service is **not** baked in and is **not** used.
- FiloDB dataset name is `prometheus`. FiloDB HTTP listens on port `8080`; it must bind `0.0.0.0` (the default `127.0.0.1` is unreachable from outside the pod). Shard status endpoint: `GET /api/v1/cluster/prometheus/status`. Dataset list endpoint: `GET /api/v1/cluster`. The active status string is exactly `ShardStatusActive`.
- `num-shards` must be less than or equal to the Kafka topic partition count. This plan uses a dedicated topic `timeseries-smoke` with 8 partitions and `num-shards = 8`.
- Off-heap block memory per pod = `num-shards` × `shard-mem-size` = 8 × 768 MB ≈ 6 GB.
- Single node: dataset `min-num-nodes = 1`, server `filodb.min-num-nodes-in-cluster = 1`, `cluster-discovery.host-list` has one entry, and the launch passes `-Dfilodb.cluster-discovery.localhost-ordinal=0`.
- All artifacts live under `deploy/kandbox/` in the FiloDB repo. The `DeploymentConfig` references sibling files by relative `source` paths that must not contain `..`.
- Work happens on the existing branch `filodb-kandbox-smoke-test`.

---

## Task 1: FiloDB single-node dataset source config

**Files:**
- Create: `deploy/kandbox/smoke/smoke-source.conf`

**Interfaces:**
- Produces: a HOCON file with `dataset = "prometheus"`, `num-shards = 8`, `min-num-nodes = 1`, ingestion topic `timeseries-smoke`, and `store.shard-mem-size = 768MB`. Task 2's server config includes this file. Task 4's script and Task 5's config depend on the topic name `timeseries-smoke`.

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/smoke/smoke-source.conf` with exactly this content:

```hocon
dataset = "prometheus"
schema = "prom-counter"

# 8 shards for the smoke test (the one-box default is 4).
# num-shards must be <= the Kafka topic partition count (we create 8).
num-shards = 8

# One FiloDB node owns all 8 shards.
min-num-nodes = 1

sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"

sourceconfig {
  filo-topic-name = "timeseries-smoke"
  bootstrap.servers = "localhost:9092"
  group.id = "filo-db-smoke-ingestion"
  shutdown-ingest-after-stopped = true

  store {
    flush-interval = 1h
    disk-time-to-live = 7 days
    max-chunks-size = 400
    max-blob-buffer-size = 15000
    # 8 shards x 768MB = ~6 GB off-heap block memory on this pod.
    shard-mem-size = 768MB
    groups-per-shard = 20
    multi-partition-odp = false
    evicted-pk-bloom-filter-capacity = 50000
    max-data-per-shard-query = 50 MB
    metering-enabled = true
    ingest-resolution-millis = 60000
    accept-duplicate-samples = false
  }
  downsample {
    resolutions = [ 1 minute, 5 minutes ]
    ttls = [ 30 days, 183 days ]
    raw-schema-names = [ "gauge", "untyped", "prom-counter", "prom-histogram"]
  }
}
```

- [ ] **Step 2: Sanity-check the file**

Run:
```bash
python3 - <<'PY'
s = open("deploy/kandbox/smoke/smoke-source.conf").read()
assert s.count("{") == s.count("}"), "unbalanced braces"
for key in ["num-shards = 8", "min-num-nodes = 1", 'filo-topic-name = "timeseries-smoke"', "shard-mem-size = 768MB", 'dataset = "prometheus"']:
    assert key in s, f"missing: {key}"
print("smoke-source.conf OK")
PY
```
Expected: `smoke-source.conf OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/kandbox/smoke/smoke-source.conf
git commit -m "feat(kandbox): FiloDB single-node 8-shard dataset source config"
```

---

## Task 2: FiloDB single-node server config

**Files:**
- Create: `deploy/kandbox/smoke/smoke-server.conf`

**Interfaces:**
- Consumes: `smoke-source.conf` (Task 1) via `include required("smoke-source.conf")`.
- Produces: a HOCON server config with `filodb.min-num-nodes-in-cluster = 1`, single-entry `cluster-discovery.host-list`, Cassandra store factory with `create-tables-enabled = true`, and the Akka bootstrap seed. Task 4's launch command points `-Dconfig.file` at this file.

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/smoke/smoke-server.conf` with exactly this content:

```hocon
dataset-prometheus = { include required("smoke-source.conf") }

filodb {
  min-num-nodes-in-cluster = 1
  v2-cluster-enabled = true

  cluster-discovery {
    failure-detection-interval = 20s
    host-list = [
      "127.0.0.1:2552"
    ]
    grpc-port-list = [
      "9901"
    ]
  }

  store-factory = "filodb.cassandra.CassandraTSStoreFactory"
  query.grpc.partitions-deny-list = "*"

  cassandra {
    hosts = "localhost"
    port = 9042
    partition-list-num-groups = 1
    create-tables-enabled = true
  }

  inline-dataset-configs = [ ${dataset-prometheus} ]

  memstore {
    ingestion-buffer-mem-size = 1GB
  }
}

akka {
  remote.netty.tcp {
    hostname = "127.0.0.1"
    port = 2552
  }
}

akka-bootstrapper {
  seed-discovery.class = "filodb.akkabootstrapper.ExplicitListClusterSeedDiscovery"
  http-seeds {
    base-url = "http://localhost:8080/"
    retries = 1
  }
  seed-discovery.timeout = 1 minute
  explicit-list.seeds = [
    "akka.tcp://filo-standalone@127.0.0.1:2552"
  ]
}
```

- [ ] **Step 2: Sanity-check the file**

Run:
```bash
python3 - <<'PY'
s = open("deploy/kandbox/smoke/smoke-server.conf").read()
assert s.count("{") == s.count("}"), "unbalanced braces"
assert s.count("[") == s.count("]"), "unbalanced brackets"
for key in ["min-num-nodes-in-cluster = 1", 'include required("smoke-source.conf")', '"127.0.0.1:2552"', "create-tables-enabled = true"]:
    assert key in s, f"missing: {key}"
# exactly one host in host-list
import re
hosts = re.search(r"host-list\s*=\s*\[(.*?)\]", s, re.S).group(1)
assert hosts.count('"') == 2, "host-list must have exactly one entry"
print("smoke-server.conf OK")
PY
```
Expected: `smoke-server.conf OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/kandbox/smoke/smoke-server.conf
git commit -m "feat(kandbox): FiloDB single-node server config (1 node, 8 shards)"
```

---

## Task 3: Console logback config

**Files:**
- Create: `deploy/kandbox/smoke/smoke-logback.xml`

**Interfaces:**
- Produces: a logback config that writes FiloDB logs to the console (stdout). The image's default logback writes to a file; this override makes logs visible via `kandbox cloud-kubectl -- logs`. Task 4's launch command points `-Dlogback.configurationFile` at this file.

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/smoke/smoke-logback.xml` with exactly this content:

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{ISO8601} %-5level [%thread] %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
```

- [ ] **Step 2: Validate the XML**

Run:
```bash
xmllint --noout deploy/kandbox/smoke/smoke-logback.xml && echo "smoke-logback.xml OK"
```
Expected: `smoke-logback.xml OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/kandbox/smoke/smoke-logback.xml
git commit -m "feat(kandbox): console logback so FiloDB logs reach pod stdout"
```

---

## Task 4: In-container startup script

**Files:**
- Create: `deploy/kandbox/smoke/run-filodb-smoke.sh`

**Interfaces:**
- Consumes: `smoke-server.conf` and `smoke-logback.xml` (mounted next to this script at `/opt/mosaic-local/smoke/`), the image's `start-mosaic.sh`, and the baked-in FiloDB jar.
- Produces: a running Cassandra + Kafka stack and a single foreground `FiloServer` on HTTP port 8080. Task 5's `DeploymentConfig` runs this script as the container command.

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/smoke/run-filodb-smoke.sh` with exactly this content:

```bash
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
```

- [ ] **Step 2: Check script syntax**

Run:
```bash
bash -n deploy/kandbox/smoke/run-filodb-smoke.sh && echo "run-filodb-smoke.sh syntax OK"
```
Expected: `run-filodb-smoke.sh syntax OK`.

- [ ] **Step 3: Make it executable and commit**

```bash
chmod +x deploy/kandbox/smoke/run-filodb-smoke.sh
git add deploy/kandbox/smoke/run-filodb-smoke.sh
git commit -m "feat(kandbox): startup script for single-node 8-shard FiloDB"
```

---

## Task 5: Kandbox DeploymentConfig

**Files:**
- Create: `deploy/kandbox/filodb-smoke-deploy-config.yaml`

**Interfaces:**
- Consumes: the four files under `deploy/kandbox/smoke/` via relative `source` paths.
- Produces: the `DeploymentConfig` that `kandbox cloud-deploy` uses (Task 8).

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/filodb-smoke-deploy-config.yaml` with exactly this content:

```yaml
apiVersion: ase.apple.com/kandbox/v1
kind: DeploymentConfig
metadata:
  name: filodb-smoke
spec:
  deploy:
    - name: filodb-onebox
      type: deployment
      image: docker.apple.com/telemetry/mosaic-local:latest
      imagePullPolicy: IfNotPresent
      replicas: 1
      stage: 0
      args:
        - "/bin/bash"
        - "/opt/mosaic-local/smoke/run-filodb-smoke.sh"
      ports:
        - name: main
          port: 8080
          appProtocol: http
      resources:
        requests:
          cpu: "4000m"
          memory: 12Gi
        limits:
          cpu: "6000m"
          memory: 16Gi
      files:
        - mountPath: /opt/mosaic-local/smoke
          items:
            - source: smoke/run-filodb-smoke.sh
              target: run-filodb-smoke.sh
              mode: "0755"
            - source: smoke/smoke-server.conf
              target: smoke-server.conf
            - source: smoke/smoke-source.conf
              target: smoke-source.conf
            - source: smoke/smoke-logback.xml
              target: smoke-logback.xml
      readinessProbe:
        scheme: http
        path: /api/v1/cluster
        port: 8080
        initialDelaySeconds: 120
        periodSeconds: 15
        timeoutSeconds: 5
        successThreshold: 1
        failureThreshold: 40
      livenessProbe:
        tcpSocket:
          port: 8080
        initialDelaySeconds: 600
        periodSeconds: 30
        timeoutSeconds: 5
        failureThreshold: 15
  readiness:
    commands:
      - "curl -sf http://$FILODB_ONEBOX_HOST:$FILODB_ONEBOX_PORT/api/v1/cluster && echo READY"
    initialDelaySeconds: 120
    periodSeconds: 15
    timeoutSeconds: 900
```

- [ ] **Step 2: Validate YAML and referenced files exist**

Run:
```bash
python3 - <<'PY'
import yaml, os
d = yaml.safe_load(open("deploy/kandbox/filodb-smoke-deploy-config.yaml"))
assert d["apiVersion"] == "ase.apple.com/kandbox/v1"
assert d["kind"] == "DeploymentConfig"
app = d["spec"]["deploy"][0]
assert app["name"] == "filodb-onebox"
assert app["image"] == "docker.apple.com/telemetry/mosaic-local:latest"
base = "deploy/kandbox"
for item in app["files"][0]["items"]:
    p = os.path.join(base, item["source"])
    assert os.path.exists(p), f"missing referenced file: {p}"
    assert ".." not in item["source"], "source must not contain .."
assert d["spec"]["readiness"]["commands"], "readiness.commands is required"
print("DeploymentConfig OK")
PY
```
Expected: `DeploymentConfig OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/kandbox/filodb-smoke-deploy-config.yaml
git commit -m "feat(kandbox): DeploymentConfig for FiloDB smoke test"
```

---

## Task 6: README with run instructions

**Files:**
- Create: `deploy/kandbox/README.md`

**Interfaces:**
- Produces: the operator runbook for Tasks 7-9 (install, create, deploy, verify, destroy).

- [ ] **Step 1: Write the file**

Create `deploy/kandbox/README.md` with exactly this content:

````markdown
# FiloDB smoke test on Kandbox

Runs a single-node FiloDB (8 shards, ~6 GB block memory) with in-sandbox
Cassandra + Kafka, using the `mosaic-local` one-box image.

## Prerequisites

- DCVPN active. AppleConnect Narrative settings on.
- Access Manager "Kandbox" access (approved).
- Install the CLI and the Claude Code plugin, then restart Claude Code:

```bash
brew install apple/amp-cloud/kandbox
```
```
/plugin marketplace add git@github.pie.apple.com:ASE-DevX/devx-skills.git
/plugin install kandbox@devx-skills
```

## Create the sandbox

```bash
kandbox cloud-create -n filodb-smoke
kandbox cloud-list
```

## Deploy

```bash
kandbox cloud-deploy -n filodb-smoke -i deploy/kandbox/filodb-smoke-deploy-config.yaml
kandbox cloud-kubectl -n filodb-smoke -- get pods
```

FiloDB needs several minutes to become Ready (starts Cassandra + Kafka, then
joins the cluster and loads shards).

## Verify

Get the pod name, then check shard status inside the pod:

```bash
POD=$(kandbox cloud-kubectl -n filodb-smoke -- get pods -l app=filodb-onebox -o jsonpath='{.items[0].metadata.name}')

# 8 shards, all ShardStatusActive:
kandbox cloud-kubectl -n filodb-smoke -- exec "$POD" -- \
  curl -s http://localhost:8080/api/v1/cluster/prometheus/status

# Kafka topic has 8 partitions:
kandbox cloud-kubectl -n filodb-smoke -- exec "$POD" -- bash -c \
  'TOPICS=$(find /opt/mosaic-local/thirdparty -name kafka-topics.sh | head -1); "$TOPICS" --describe --zookeeper localhost:2181 --topic timeseries-smoke'

# Block-memory sizing in logs:
kandbox cloud-kubectl -n filodb-smoke -- logs "$POD" | grep -iE "block memory|blockmanager|shard-mem|memory-alloc" | head
```

## Destroy

```bash
kandbox cloud-destroy -n filodb-smoke
```

## Notes

- The query service is not included; verification uses the FiloServer HTTP API on
  port 8080 directly.
- The readiness command in the DeploymentConfig references
  `$FILODB_ONEBOX_HOST` / `$FILODB_ONEBOX_PORT`. If Kandbox exposes the endpoint
  under a different variable name, run `kandbox cloud-export -n filodb-smoke` to
  see the exact names and adjust.
````

- [ ] **Step 2: Commit**

```bash
git add deploy/kandbox/README.md
git commit -m "docs(kandbox): runbook for FiloDB smoke test"
```

---

## Task 7: Install tooling and create the sandbox (interactive — human/Claude-with-plugin, not a subagent)

This task needs the live environment (DCVPN, `kandbox` CLI, cloud sandbox). It cannot run in a subagent.

- [ ] **Step 1: Install the CLI and plugin**

```bash
brew install apple/amp-cloud/kandbox
kandbox version
```
Expected: a version string prints.

Then in Claude Code:
```
/plugin marketplace add git@github.pie.apple.com:ASE-DevX/devx-skills.git
/plugin install kandbox@devx-skills
```
Restart Claude Code when prompted. After restart, the Kandbox MCP tools are available.

- [ ] **Step 2: Create the sandbox**

```bash
kandbox cloud-create -n filodb-smoke
```
Expected: the command succeeds and prints the sandbox namespace.

- [ ] **Step 3: Confirm it exists**

```bash
kandbox cloud-list
```
Expected: `filodb-smoke` appears in the list.

---

## Task 8: Deploy and wait for the pod (interactive)

- [ ] **Step 1: Deploy**

```bash
kandbox cloud-deploy -n filodb-smoke -i deploy/kandbox/filodb-smoke-deploy-config.yaml
```
Expected: the deploy command reports success or begins rollout.

- [ ] **Step 2: Watch the pod reach Running**

```bash
kandbox cloud-kubectl -n filodb-smoke -- get pods
```
Expected: one `filodb-onebox` pod, `STATUS=Running`, restarts staying at 0. If it is `CrashLoopBackOff` or exits 189, read logs (Step 3) and use the `filodb-debugging` skill.

- [ ] **Step 3: Tail startup logs**

```bash
POD=$(kandbox cloud-kubectl -n filodb-smoke -- get pods -l app=filodb-onebox -o jsonpath='{.items[0].metadata.name}')
kandbox cloud-kubectl -n filodb-smoke -- logs "$POD" --tail=50
```
Expected: `[smoke] ...` lines showing Cassandra, Kafka, topic creation, then FiloServer startup.

---

## Task 9: Verify success criteria and clean up (interactive)

- [ ] **Step 1: Assert 8 shards ACTIVE**

```bash
POD=$(kandbox cloud-kubectl -n filodb-smoke -- get pods -l app=filodb-onebox -o jsonpath='{.items[0].metadata.name}')
kandbox cloud-kubectl -n filodb-smoke -- exec "$POD" -- \
  curl -s http://localhost:8080/api/v1/cluster/prometheus/status
```
Expected: JSON with `"status": "success"` and `data` containing 8 entries, each with `"status": "ShardStatusActive"`. Shards may pass through `ShardStatusRecovery(...)` first; re-run until all 8 are Active (allow several minutes).

- [ ] **Step 2: Assert the topic has 8 partitions**

```bash
kandbox cloud-kubectl -n filodb-smoke -- exec "$POD" -- bash -c \
  'TOPICS=$(find /opt/mosaic-local/thirdparty -name kafka-topics.sh | head -1); "$TOPICS" --describe --zookeeper localhost:2181 --topic timeseries-smoke'
```
Expected: `PartitionCount: 8` for topic `timeseries-smoke`.

- [ ] **Step 3: Confirm the block-memory budget in logs**

```bash
kandbox cloud-kubectl -n filodb-smoke -- logs "$POD" | grep -iE "block memory|blockmanager|shard-mem|memory-alloc" | head
```
Expected: log lines referencing block-memory sizing consistent with ~6 GB (8 shards × 768 MB). This is a soft check; if the exact wording differs, confirm the config value `shard-mem-size = 768MB` and `num-shards = 8` are in effect.

- [ ] **Step 4: Destroy the sandbox**

```bash
kandbox cloud-destroy -n filodb-smoke
kandbox cloud-list
```
Expected: `filodb-smoke` no longer appears.

---

## Self-review

**Spec coverage:**
- Cloud sandbox, in-sandbox Cassandra + Kafka + FiloDB → Tasks 5, 7, 8.
- 8 shards → `num-shards = 8` (Task 1), asserted in Task 9 Step 1.
- ~6 GB block memory → `shard-mem-size = 768MB` × 8 (Task 1), checked in Task 9 Step 3.
- Single node → `min-num-nodes` / `min-num-nodes-in-cluster = 1` + single host-list + `localhost-ordinal=0` (Tasks 1, 2, 4).
- 8-partition topic = num-shards → Task 4, asserted Task 9 Step 2.
- Query service dropped → not used; verify via FiloDB HTTP (Global Constraints, Task 9).
- Prereqs (CLI + plugin + DCVPN) → Task 7, README.
- Disposable → `cloud-destroy` (Task 9 Step 4).

**Placeholder scan:** No TBD/TODO. Every file has full content. The block-memory log check is explicitly a soft check with a config-based fallback assertion, not a placeholder.

**Type/name consistency:** Topic `timeseries-smoke`, dataset `prometheus`, app `filodb-onebox`, port `8080`, status string `ShardStatusActive`, base dir `/opt/mosaic-local`, mount dir `/opt/mosaic-local/smoke`, jar `/opt/mosaic-local/filodb/lib/filodb-pie-assembly.jar` — used consistently across Tasks 1-9.

## Risks

- Cloud-sandbox quota may reject a single 16 Gi pod. Mitigation: lower `shard-mem-size` (for example 512 MB × 8 = 4 GB) and the memory limit, then note the reduced budget.
- `start-mosaic.sh startKafka` uses `nc localhost 2181` to wait on Zookeeper. If `nc` is absent in the image, that step hangs. Mitigation: CI runs this image, so `nc` should be present; if not, install it or replace the wait.
- FiloDB shards may sit in `ShardStatusRecovery` before turning Active. Mitigation: re-poll (Task 9 Step 1); allow several minutes.
- The readiness command's endpoint env-var names may differ. Mitigation: `kandbox cloud-export` (noted in README).
