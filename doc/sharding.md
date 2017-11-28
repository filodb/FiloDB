<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [FiloDB](#filodb)
  - [Shard Coordination](#shard-coordination)
  - [Shard Assignment](#shard-assignment)
    - [Shard Event Subscriptions](#shard-event-subscriptions)
      - [Subscribe to Shard Status Events](#subscribe-to-shard-status-events)
        - [Shard Status and Shard Status Events](#shard-status-and-shard-status-events)
        - [Auto Subscribe](#auto-subscribe)
      - [Unsubscribe to Shard Status Events](#unsubscribe-to-shard-status-events)
        - [Auto Unsubscribe](#auto-unsubscribe)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# FiloDB

## Shard Coordination
FiloDB Clients enable users to set up new datasets as needed. Internally clients send a `SetupDataset` command 
to the [NodeClusterActor](../coordinator/src/main/scala/filodb.coordinator/NodeClusterActor.scala). 
This action provisions the new dataset and adds it as a subscription users can subscribe
to and receive shard change events on for that dataset. The `NodeClusterActor` provides the 
API for client interaction with the shard information per dataset. 

The [ShardCoordinatorActor](../coordinator/src/main/scala/filodb.coordinator/ShardCoordinatorActor.scala) 
manages the following for its parent, the cluster singleton, 
[NodeClusterActor](../coordinator/src/main/scala/filodb.coordinator/NodeClusterActor.scala)

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
   
## Shard Assignment
A [ShardAssignmentStrategy](../coordinator/src/main/scala/filodb.coordinator/ShardAssignmentStrategy.scala) is responsible for assigning or removing shards to/from nodes based on some
policy, when state changes occur. Initial shards are assigned by the configured `ShardAssignmentStrategy`.

In the `DefaultShardAssignmentStrategy` implementation, shard assignment is based on the number of healthy 
member nodes in a FiloDB cluster, the shard to node ratio, and the number of unassigned shards. This 
strategy will allocate resources to a dataset when a minimum of N nodes are up. It is informed
on each node added and removed by the `ShardCoordinatorActor`. Custom implementations can be configured. 
   
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
The `ShardCoordinatorActor` (child of cluster singleton) acks the subscriber request 
with a `CurrentShardSnapshot(ref: DatasetRef, latestMap: ShardMapper)`. `CurrentShardSnapshot` is sent once to newly-subscribed subscribers to initialize their local `ShardMapper`.

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
internally by FiloDB via the `ShardCoordinatorActor` and `ShardAssignmentStrategy`. Subscribers
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

##### Auto Subscribe
On `MemberUp` each new `NodeCoordinatorActor` is automatically subscribed to all datasets added. Then 
the `NodeCoordinatorActor` can receive `ShardCommand`s which it forwards to the appropriate Ingester, 
e.g. to start shard ingestion for that dataset on that shard. A successful start of that ingestion stream
will broadcast an `IngestionStarted` event to all subscribers of that dataset.

#### Unsubscribe to Shard Status Events
A subscriber can unsubscribe at any time by sending the `ShardCoordinatorActor` a `Unsubscribe(self)` command,
which will unsubscribe from all the subscriber's subscriptions. Unsubscribe happens automatically when a subscriber actor
is terminated for any reason. This is also true for all `NodeCoordinatorActor`s.

##### Auto Unsubscribe
Any subscriber that has terminated is automatically removed.
