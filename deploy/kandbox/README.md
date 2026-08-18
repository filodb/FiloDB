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
