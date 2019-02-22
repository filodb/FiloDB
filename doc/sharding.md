<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Data Sharding](#data-sharding)
  - [The Shard Key](#the-shard-key)
  - [Spread, or How to Avoid Hotspotting](#spread-or-how-to-avoid-hotspotting)
  - [Shard Coordination](#shard-coordination)
  - [Shard Assignment](#shard-assignment)
    - [Shard Event Subscriptions](#shard-event-subscriptions)
      - [Subscribe to Shard Status Events](#subscribe-to-shard-status-events)
        - [Shard Status and Shard Status Events](#shard-status-and-shard-status-events)
      - [Unsubscribe to Shard Status Events](#unsubscribe-to-shard-status-events)
      - [Auto Unsubscribe](#auto-unsubscribe)
  - [Cluster/Shard State Recovery](#clustershard-state-recovery)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB

## Data Sharding

Incoming data samples are divided into shards, typically by the ingestion source such as Apache Kafka, where 1 shard == 1 partition.   Currently the total number of shards must be a power of two, such as 128 or 256.  Right now, the number of partitions in a Kafka topic should match the number of shards.

Each shard is wholly stored and contained within one FiloDB node.  A central cluster coordinator running on one of the FiloDB nodes (the `NodeClusterActor` and `ShardManager` classes) allocates shards to individual FiloDB nodes.  

The total number of shards for a cluster is not currently changeable, it must be configured properly when sizing a cluster initially.  The number of nodes in a FiloDB cluster can come and go, and shard assignments may change when a node is introduced or fails and/or leaves the cluster.  Since a shard must fit within one node, typically one would allocate many more shards than the expected cluster size.  For example, we might allocate 256 shards to a cluster expected to be around 32 nodes on average.  This allows for some dynamic growth in the cluster size without a huge impact on the number of shards on any node.

## The Shard Key

Both the ingestion gateway and the query layer must agree on the same strategy for deriving the shards from incoming records and query requests.  This is done using a "shard key" composed of specific columns and/or keys/tags from a `MapColumn`.   For example, for Prometheus schema data, we might decide to use the keys `__name__` (the metric name) and `job` to form the shard key.

What the above means is that

1. On ingest, the `__name__` and `job` are used to decide (in combination with other "partition key" fields) the specific shard / Kafka partition that data goes into
2. On query, both `__name__` and `job` must be present as filters and is used to form the range of shards to direct queries at.

## Spread, or How to Avoid Hotspotting

The **spread** determines how many shards a given shard key is mapped to.  The number of shards is equal to 2 to the power of the spread.  It is used to manage how widely specific shard keys (such as applications, the job, or metrics) are distributed.  For example, if one job or metric has a huge number of series, one can assign a higher spread to it to avoid hotspotting.  (The management of spreads for individual shard keys is not currently included in the open source offering).

## Shard Coordination
FiloDB Clients enable users to set up new datasets as needed. Internally clients send a `SetupDataset` command 
to the [NodeClusterActor](../coordinator/src/main/scala/filodb.coordinator/NodeClusterActor.scala). 
This action provisions the new dataset and adds it as a subscription users can subscribe
to and receive shard change events on for that dataset. The `NodeClusterActor` provides the 
API for client interaction with the shard information per dataset. 

The [NodeClusterActor](../coordinator/src/main/scala/filodb.coordinator/NodeClusterActor.scala) delegates shard 
coordination operations to [ShardManager](../coordinator/src/main/scala/filodb.coordinator/ShardManager.scala) 
a synchronous helper class that is responsible for orchestrating the shard assignment events. It also is the owner
for the shard assignment state within the NodeClusterActor. It manages:

* The [ShardMapper(s)](../coordinator/src/main/scala/filodb.coordinator/ShardMapper.scala) for each dataset
* The Subscription for each dataset to publish shard state events to subscribers
* Current Subscribers for each dataset - via Deathwatch
* Publishes [ShardEvent(s)](../coordinator/src/main/scala/filodb.coordinator/ShardStatus.scala#L51-L63)
to subscribers of the shard event's dataset on behalf of [Ingesters](../coordinator/src/main/scala/filodb.coordinator/MemStoreCoordActor.scala)
 
ShardMappers internally manage
* The mapping for a single dataset and its shards and the node those shards reside on
* The shard status for a single dataset
* The number of assigned and unassigned shards

And other related dataset shard state. Commands are sent by the `NodeClusterActor` 
to the right nodes upon events or changes to the cluster. 
For example a new node joins, StartShardIngestion might be sent.

To better understand interactions between NodeClusterActor, ShardManager, ShardAssignmentStrategy and NodeCoordinator
see the documented [Shard Coordination Sequence Diagram](mermaid/shard-coordination.mermaid.png) 
   
## Shard Assignment

The ShardManager delegates shard assignment decisions to a [ShardAssignmentStrategy](../coordinator/src/main/scala/filodb.coordinator/ShardAssignmentStrategy.scala) 
implementation. This trait has one method, that makes shard assignment recommendation for one dataset and one worker
node at a time. The ShardManager is responsible for sequencing the assignment recommendations for all worker nodes
whenever shards need to be assigned to nodes. It then makes the state change based on these recommendations. It is 
important that state change be applied for one node prior to fetching recommendations for next node. This is important
since the assignment strategy is designed to be functional and stateless. The assignment state is owned by the 
ShardManager. 

In the `DefaultShardAssignmentStrategy` implementation, shards are eagerly assigned to nodes after making sure that
shards are evenly spread as much as possible among the nodes. If shards go down, and reassignment needs to be done,
the assignment strategy will also try to find room on nodes that were incompletely filled.

The ShardManager must seek recommendations for assignment in reverse order of deployment in order to aid smooth
rolling upgrades, which will be carried out by bringing up a new version node before bringing down an old version node.
More recently deployed nodes are given preference for shard assignment. 

### Shard Event Subscriptions
It is possible to subscribe to shard events for any user-defined datasets, for example to know when an ingestion stream
has started for a dataset, is down, encountered an error, is recovering or has stopped. This functionality allows
several things including:

* Dynamic awareness of the health of all dataset shard streams
* QueryActors to receive updated status of every shard so it knows where to route queries to (or to fail)
* Clients sending records in (via pushing IngestRows into NodeCoordinator) to receive events or status updates to know when it can start sending
* Ingestors ([Ingesters](../coordinator/src/main/scala/filodb.coordinator/MemStoreCoordActor.scala)) to publish status updates (e.g. ERROR, or ingestion started)
* Ingestors ([Ingesters](../coordinator/src/main/scala/filodb.coordinator/MemStoreCoordActor.scala)) to receive shard commands and know when to stop ingestion, or when to start ingesting

#### Subscribe to Shard Status Events
To subscribe to shard events, a client sends a `SubscribeShardUpdates(ref: DatasetRef)` to the `NodeClusterActor` (a cluster singleton).
The `ShardManager` acks the subscriber request with a `CurrentShardSnapshot(ref: DatasetRef, latestMap: ShardMapper)`. `CurrentShardSnapshot` is sent once to newly-subscribed subscribers to initialize their local `ShardMapper`.

```
clusterActor ! SubscribeShardUpdates(dataset)
```

The subscriber will receive shard events as they occur, for any datasets they subscribe to per `SubscribeShardUpdates`.

The association of ShardEvents to ShardStatus (api in progress) starting with ShardStatusUnassigned:
```
  case ShardAssignmentStarted(dataset, shard, node) => // ShardStatusAssigned
  case IngestionStarted(dataset, shard, node)       => // ShardStatusNormal
  case IngestionError(dataset, shard, ex)           => // ShardStatusError
  case IngestionStopped(dataset, shard)             => // ShardStatusStopped
  case RecoveryStarted(dataset, shard, node)        => // ShardStatusRecovery
  case ShardDown(dataset, shard, node)              => // ShardStatusDown (changing, breaking out to more granular)
  case ShardMemberRemoved(dataset, shard, node)     => // ShardMemberRemoved (also changing)
}
```
 
##### Shard Status and Shard Status Events
The [Shard Ingestion Events(s)](../coordinator/src/main/scala/filodb.coordinator/ShardStatus.scala#L51-L63) are sent per dataset stream Ingester actors when the ingestion stream
starts, errors, or by the `NodeClusterActor` singleton upon detection of node failure / disconnect via Akka Cluster events.
These events are subscribed to by QueryActors, etc. For example in Spark, executors waiting to know when they can start sending records. 
      
Each `ShardMapper` maps each shard to the ActorRef of the `NodeCoordinatorActor` of the FiloDB node holding that shard. 
The [Shard Status](../coordinator/src/main/scala/filodb.coordinator/ShardStatus.scala#L65-L94) of the shards are updated
internally by FiloDB via `ShardManager`. Subscribers
can maintain their own local copy for a dataset if needed, and leverage the events:

```
def receive: Actor.Receive = {
  case CurrentShardSnapshot(_, shardMap) =>
    localMap = shardMap
  case e: ShardEvent                     =>
    localMap.updateFromEvent(e)
    // now you can leverage the current state of all nodes, all shards for a given dataset
    // via the `ShardMapper` API
}
```
##### Automatic Reassignment of Shards
The Cluster Singleton assumes that the IngestionActor and the Ingest stream handles all recoverable errors.
If an exception is thrown, the error is not recoverable.

When an unrecoverable exception occurs in the ingestion stream, the IngestionActor releases resources
for the shard, and an IngestionError event is sent to the singleton.

The ShardManager then attempts to reassign the shard to another node. This reassignment is done only if
the shard was not previously reassigned within 2 hours (configurable). This is to prevent errors arising from
data or load causing shard to be continuously reassigned.

#### Unsubscribe to Shard Status Events
A subscriber can unsubscribe at any time by sending the `NodeClusterActor` a `Unsubscribe(self)` command,
which will unsubscribe from all the subscriber's subscriptions. Unsubscribe happens automatically when a subscriber actor
is terminated for any reason. This is also true for all `NodeCoordinatorActor`s.

#### Auto Unsubscribe
Any subscriber that has terminated is automatically removed.

## Cluster/Shard State Recovery

The `NodeClusterActor` and `ShardManager` are both singletons and only one instance lives in the Akka/FiloDB Cluster at any one time.  They contain important state such as shard maps and current subscriptions to shard status updates.  What if the node containing the singleton dies?  

Here is what happens:

1. When each node starts up, its `NodeGuardian` subscribes to all shard status and subscription updates from the singletons.
2. As the shard status updates and new subscribers come online, the `ShardManager` sends updates to all the `NodeGuardian` subscribers.
3. The node containing the singletons goes down.  Akka sends `Unreachable` and other cluster membership events, though this won't be noticed because the node is down.
4. Eventually, Akka's ClusterSingletonManager notices the node is down and restarts the singletons on a different node.  When it is restarted, it has none of the previous state.
5. The `NodeClusterActor` first recovers previous streaming ingestion configs from the `MetaStore`, which is probably Cassandra.
6. The `NodeClusterActor` next recovers the current shard maps and subscribers from the new node's `NodeGuardian`, which should have been subscribing to updates due to the first step.
7. Next, the `NodeClusterActor` replays a summary of all cluster membership events, including any new node failure events, and thus updates shard status accordingly.
