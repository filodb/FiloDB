<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB FAQ](#filodb-faq)
  - [Q: FiloDB supports Prometheus style queries, but why shouldn't I just use Prometheus?](#q-filodb-supports-prometheus-style-queries-but-why-shouldnt-i-just-use-prometheus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB FAQ

## Q: FiloDB supports Prometheus style queries, but why shouldn't I just use Prometheus?

At its core, FiloDB is distributed, scalable, and fault-tolerant. Prometheus stores data in a single node, and it lacks data replication features. With Prometheus, loss of a node can cause loss of data.
