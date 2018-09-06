# FiloDB FAQ

## Q: FiloDB supports Prometheus style queries, but why shouldn't I just use Prometheus?

At its core, FiloDB is distributed, scalable, and fault-tolerant. Prometheus stores data in a single node, and it lacks data replication features. With Prometheus, loss of a node can cause loss of data.
