package filodb.coordinator

import scala.collection.mutable
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, Address}
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.NodeClusterActor._
import filodb.core.DatasetRef
import filodb.core.metadata.Dataset

/**
  * NodeClusterActor delegates shard management business logic to this class.
  * It is the home for shard assignment state (shard mappers) for all datasets,
  * and is responsible for mutating them based on cluster membership events and
  * dataset add/remove operations.
  *
  * This class also ensures that shard assignment to nodes are optimal and ensures
  * maximum number of shards are "available" for service at any given time.
  *
  * This class currently handles shard event subscriptions too, but:
  * TODO: Move Subscription logic outside of this class into a separate helper class.
  */
private[coordinator] final class ShardManager(strategy: ShardAssignmentStrategy) extends StrictLogging {

  import ShardManager._

  private var _subscriptions = ShardSubscriptions(Set.empty, Set.empty)
  private val _datasetInfo = new mutable.HashMap[DatasetRef, DatasetInfo]
  private val _shardMappers = new mutable.HashMap[DatasetRef, ShardMapper]
  // preserve deployment order - newest last
  private val _coordinators = new mutable.LinkedHashMap[Address, ActorRef]

  /* These workloads were in an actor and exist now in an unprotected class.
  Do not expose mutable datasets. Internal work always uses the above datasets,
  these are for users, or tests use them, and are called infrequently. */
  def subscriptions: ShardSubscriptions = _subscriptions
  def datasetInfo: Map[DatasetRef, DatasetInfo] = _datasetInfo.toMap
  def shardMappers: Map[DatasetRef, ShardMapper] = _shardMappers.toMap
  def coordinators: Seq[ActorRef] = _coordinators.values.toSeq

  /** Subscribes the internal actor to shard events and sends current
    * snapshot of subscribers per dataset. This `subsce`
    */
  def subscribeAll(subscriber: ActorRef): Unit = {
    logger.info(s"Subscribing $subscriber to events from all datasets and subscriptions")
    _subscriptions = subscriptions subscribe subscriber
    _subscriptions.watchers foreach (_ ! subscriptions)
    // send the subscriber all current shardMappers
    _shardMappers foreach { case (ref, map) => subscriber ! CurrentShardSnapshot(ref, map) }
  }

  /** If the mapper for the provided `datasetRef` has been added, sends an initial
    * current snapshot of partition state, as ingestion will subscribe usually when
    * the cluster is already stable.
    *
    * This function is called in two cases: when a client sends the cluster actor
    * a `SubscribeShardUpdates`, and when a coordinator creates the memstore
    * and query actor for a newly-registered dataset and sends the shard actor
    * a subscribe for the query actor. In the first case there is no guarantee
    * that the dataset is setup, in the second there is.
    *
    * INTERNAL API. Idempotent.
    */
  def subscribe(subscriber: ActorRef, dataset: DatasetRef): Unit =
    mapperOpt(dataset) match {
      case Some(current) =>
        logger.info(s"Adding $subscriber as a subscriber for dataset $dataset")
        _subscriptions = subscriptions.subscribe(subscriber, dataset)
        _subscriptions.watchers foreach (_ ! subscriptions)
        subscriber ! current
      case _ =>
        logger.error(s"Dataset $dataset unknown, unable to subscribe $subscriber.")
        subscriber ! DatasetUnknown(dataset)
    }

  /**
    * Unsubscribes a subscriber from all dataset shard updates.
    * Sends watchers the updated subscriptions.
    * INTERNAL API. Idempotent.
    *
    * @param subscriber the cluster member removed from the cluster
    *                   or regular subscriber unsubscribing
    */
  def unsubscribe(subscriber: ActorRef): Unit = {
    _subscriptions = subscriptions unsubscribe subscriber
    _subscriptions.watchers foreach (_ ! subscriptions)
  }

  /** Sends subscribers for the dataset to the requester. If the subscription
    * does not exist the subscribers will be empty.
    *
    * INTERNAL API. Read-only.
    */
  private def getSubscribers(ds: DatasetRef): Set[ActorRef] =
    _subscriptions.subscribers(ds)

  /** Resets all state except for coord list.
    * INTERNAL API.
    */
  def reset(): Unit = {
    _datasetInfo.values.foreach(_.metrics.reset())
    _datasetInfo.clear()
    _shardMappers.clear()
    _subscriptions = subscriptions.clear
  }

  /** Here the origin can be a client, forwarded from the `NodeClusterActor`.
    * The response is returned directly to the requester.
    */
  def sendSnapshot(ref: DatasetRef, origin: ActorRef): Unit =
    origin ! mapperOpt(ref).getOrElse(DatasetUnknown(ref))

  private def mapperOpt(ref: DatasetRef): Option[CurrentShardSnapshot] =
    _shardMappers.get(ref).map(m => CurrentShardSnapshot(ref, m))

  /** Selects the `ShardMapper` for the provided dataset, updates the mapper
    * for the received shard event from the event source.
    */
  def updateFromShardEventNoPublish(e: ShardEvent): Unit =
    _shardMappers.get(e.ref) foreach (m => m.updateFromEvent(e))

  /** Called on MemberUp. Handles acquiring assignable shards, if any, assignment,
    * and full setup of new node.
    *
    * @param address the `akka.cluster.Cluster.selfAddress` of the node
    * @param coordinator the node coordinator
    */
  def addMember(address: Address, coordinator: ActorRef): Unit = {
    logger.info(s"Initiated addMember for coordinator $coordinator")
    _coordinators(address) = coordinator

    for ((dataset, resources, mapper) <- datasetShardMaps) {
      val assignable = strategy.shardAssignments(coordinator, dataset, resources, mapper)
      if (assignable.nonEmpty) sendAssignmentMessagesAndEvents(dataset, coordinator, assignable)
    }
    logger.info(s"Completed addMember for coordinator $coordinator")
  }

  /** Called on MemberRemoved, new status already updated. */
  def removeMember(address: Address): Option[ActorRef] =
    _coordinators.get(address) map { coordinator =>
        logger.info(s"Initiated removeMember for coordinator on $address")
        _coordinators remove address
        removeCoordinator(coordinator)
        coordinator
    }

  private def updateShardMetrics(): Unit = {
    _datasetInfo.foreach { case (dataset, info) =>
        info.metrics.update(_shardMappers(dataset))
    }
  }

  /**
    * Called after recovery of cluster singleton to remove assignment of stale member(s).
    * This is necessary after fail-over of the cluster singleton node because MemberRemoved
    * for failed singleton nodes are not consistently delivered to the node owning the new singleton.
    */
  def removeStaleCoordinators(): Unit = {
    logger.info("Attempting to remove stale coordinators from cluster")
    val nodesToRemove = for {
      (dataset, mapper) <- shardMappers
    } yield {
      val allRegisteredNodes = mapper.allNodes
      allRegisteredNodes -- coordinators // coordinators is the list of recovered nodes
    }
    for { coord <- nodesToRemove.flatten } {
      logger.info(s"Cleaning up stale coordinator $coord after recovery")
      removeCoordinator(coord)
    }
    updateShardMetrics()
  }

  private def removeCoordinator(coordinator: ActorRef): Unit = {
    for ((dataset, resources, mapper) <- datasetShardMaps) {
      sendUnassignmentMessagesAndEvents(dataset, coordinator, mapper)
      // try to reassign shards that were unassigned to other nodes that have room.
      assignShardsToNodes(dataset, mapper, resources)
    }
    logger.info(s"Completed removeMember for coordinator $coordinator")
  }

  /**
    * Adds new dataset to cluster, thereby initiating new shard assignments to existing nodes
    * @return new assignments that were made. Empty if dataset already exists.
    */
  def addDataset(setup: SetupDataset,
                 dataset: Dataset,
                 ackTo: ActorRef): Map[ActorRef, Seq[Int]] = {

    logger.info(s"Initiated Setup for Dataset ${setup.ref}")
    val answer: Map[ActorRef, Seq[Int]] = mapperOpt(setup.ref) match {
      case Some(_) =>
        logger.info(s"Dataset ${setup.ref} already exists - ignoring")
        ackTo ! DatasetExists(setup.ref)
        Map.empty
      case None =>
        val mapper = new ShardMapper(setup.resources.numShards)
        _shardMappers(dataset.ref) = mapper
        // Access the shardmapper through the HashMap so even if it gets replaced it will update the shard stats
        val metrics = new ShardHealthStats(setup.ref, _shardMappers(dataset.ref))
        val resources = setup.resources
        val source = setup.source
        val state = DatasetInfo(resources, metrics, source, dataset)
        _datasetInfo(dataset.ref) = state

        val assignments = assignShardsToNodes(dataset.ref, mapper, resources)

        _subscriptions :+= ShardSubscription(dataset.ref, Set.empty)
        _subscriptions.watchers foreach (subscribe(_, dataset.ref))
        _subscriptions.watchers foreach (_ ! CurrentShardSnapshot(dataset.ref, mapper))
        logger.info(s"Completed Setup for Dataset ${setup.ref}")
        ackTo ! DatasetVerified
        assignments
    }
    updateShardMetrics()
    answer
  }

  private def assignShardsToNodes(dataset: DatasetRef,
                                  mapper: ShardMapper,
                                  resources: DatasetResourceSpec): Map[ActorRef, Seq[Int]] = {
    (for {
      coord <- latestCoords // assign shards on newer nodes first
    } yield {
      val assignable = strategy.shardAssignments(coord, dataset, resources, mapper)
      if (assignable.nonEmpty) sendAssignmentMessagesAndEvents(dataset, coord, assignable)
      coord -> assignable
    }).toMap
  }

  def removeDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Initiated removal for Dataset $dataset")
    for {
      (_, coord) <- _coordinators
      mapper = _shardMappers(dataset)
    } sendUnassignmentMessagesAndEvents(dataset, coord, mapper)
    _datasetInfo remove dataset
    _shardMappers remove dataset
    _subscriptions = _subscriptions - dataset
    logger.info(s"Completed removal for Dataset $dataset")
  }

  /**
    * Intended for recovery of ShardMapper state only - recovers a current ShardMap as well as updating a list of
    * members / coordinator ActorRefs
    */
  def recoverShards(ref: DatasetRef, map: ShardMapper): Unit = {
    logger.info(s"Recovering map for dataset $ref")
    _shardMappers(ref) = map
    logger.debug(s"Map contents: $map")
    updateShardMetrics()
  }

  def recoverSubscriptions(subs: ShardSubscriptions): Unit = {
    logger.info(s"Recovering (adding) subscriptions from $subs")
    _subscriptions = subscriptions.copy(subscriptions = _subscriptions.subscriptions ++ subs.subscriptions,
      watchers = _subscriptions.watchers ++ subs.watchers)
    logger.debug(s"Recovered subscriptions = $subscriptions")
  }

  /** Selects the `ShardMapper` for the provided dataset, updates the mapper
    * for the received shard event from the event source, and publishes
    * the event to all subscribers of that event and dataset.
    */
  def updateFromShardEventAndPublish(event: ShardEvent): Unit = {
    _shardMappers.get(event.ref) foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"Invalid shard.", l)
        case Success(r) =>
          logger.info(s"Updated mapper for dataset ${event.ref} event $event")
      }
      // TODO if failure we don't need to publish, though that's what we have
      // been doing thus far. This requires changing tests out of scope for the current changes
      publishEvent(event)
      logger.debug(s"Shard Mapper after updateFromShardEventAndPublish: $mapper")
    }
    updateShardMetrics()
  }

  /**
    * This method has the shared logic for sending shard assignment messages
    * to the coordinator, updating state for the event and broadcast of the state change to subscribers
    */
  private def sendAssignmentMessagesAndEvents(dataset: DatasetRef,
                                              coord: ActorRef,
                                              shards: Seq[Int]): Unit = {
    val state = _datasetInfo(dataset)
    logger.info(s"Sending setup message for ${state.dataset.ref} to coordinators $coord.")
    val setupMsg = client.IngestionCommands.DatasetSetup(state.dataset.asCompactString, state.source)
    coord ! setupMsg

    for { shard <- shards }  {
      val event = ShardAssignmentStarted(dataset, shard, coord)
      updateFromShardEventAndPublish(event)
    }
    /* If no shards are assigned to a coordinator, no commands are sent. */
    logger.info(s"Sending start ingestion message for $dataset to coordinator $coord for shards $shards")
    for {shard <- shards} coord ! StartShardIngestion(dataset, shard, None)
  }

  /**
    * This method has the shared logic for sending shard un-assignment messages
    * to the coordinator, updating state for the event and broadcast of the state change to subscribers
    */
  private def sendUnassignmentMessagesAndEvents(dataset: DatasetRef,
                                                coordinator: ActorRef,
                                                mapper: ShardMapper,
                                                nodeUp: Boolean = true): Unit = {
    val shardsToDown = mapper.shardsForCoord(coordinator)
    logger.info(s"Sending stop ingestion message for $dataset to coordinator $coordinator for shards $shardsToDown")
    for { shard <- shardsToDown } {
      val event = ShardDown(dataset, shard, coordinator)
      updateFromShardEventAndPublish(event)
      if (nodeUp) coordinator ! StopShardIngestion(dataset, shard)
    }
    logger.debug(s"Unassigned shards $shardsToDown from $coordinator")
  }

  /** Publishes the event to all subscribers of that dataset. */
  private def publishEvent(e: ShardEvent): Unit =
    for {
      subscription <- _subscriptions.subscription(e.ref)
    } subscription.subscribers foreach (_ ! e)

  private def latestCoords: Seq[ActorRef] =
    _coordinators.values.foldLeft(List[ActorRef]())((x, y) => y :: x) // reverses the set

  private def datasetShardMaps: Iterable[(DatasetRef, DatasetResourceSpec, ShardMapper)] =
    for {
      (dataset, state) <- _datasetInfo
      mapper = _shardMappers(dataset)
      resources = state.resources
    } yield (dataset, resources, mapper)

}

private[coordinator] object ShardManager {

  final case class DatasetInfo(resources: DatasetResourceSpec,
                               metrics: ShardHealthStats,
                               source: IngestionSource,
                               dataset: Dataset)
}