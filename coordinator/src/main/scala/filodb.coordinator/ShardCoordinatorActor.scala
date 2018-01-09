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
final private[coordinator] class ShardManager(strategy: ShardAssignmentStrategy) extends StrictLogging {

  case class DatasetInfo(resources: DatasetResourceSpec,
                         metrics: ShardHealthStats,
                         source: IngestionSource,
                         dataset: Dataset)

  private[coordinator] var subscriptions = ShardSubscriptions(Set.empty, Set.empty)
  private[coordinator] val datasetInfo = new mutable.HashMap[DatasetRef, DatasetInfo]
  private[coordinator] val shardMappers = new mutable.HashMap[DatasetRef, ShardMapper]

  // preserve deployment order - newest last
  private[coordinator] val coords = new mutable.LinkedHashSet[ActorRef]

  /** Subscribes the internal actor to shard events and sends current
    * snapshot of subscribers per dataset. This `subsce`
    */
  def subscribeAll(subscriber: ActorRef): Unit = {
    logger.info(s"Subscribing $subscriber to events from all datasets and subscriptions")
    subscriptions = subscriptions subscribe subscriber
    subscriptions.watchers foreach (_ ! subscriptions)
    // send the subscriber all current shardMappers
    shardMappers foreach { case (ref, map) => subscriber ! CurrentShardSnapshot(ref, map) }
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
  def subscribe(subscriber: ActorRef, dataset: DatasetRef): Unit = {
    mapperOpt(dataset) match {
      case Some(current) =>
        logger.info(s"Adding ${subscriber} as a subscriber for dataset ${dataset}")
        subscriptions = subscriptions.subscribe(subscriber, dataset)
        subscriptions.watchers foreach (_ ! subscriptions)
        //        context watch subscriber
        subscriber ! current
      case _ =>
        logger.error(s"Dataset ${dataset} unknown, unable to subscribe ${subscriber}.")
        subscriber ! DatasetUnknown(dataset)
    }
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
    subscriptions = subscriptions unsubscribe subscriber
    subscriptions.watchers foreach (_ ! subscriptions)
  }

  /** Sends subscribers for the dataset to the requester. If the subscription
    * does not exist the subscribers will be empty.
    *
    * INTERNAL API. Read-only.
    */
  private def getSubscribers(ds: DatasetRef): Set[ActorRef] = subscriptions.subscribers(ds)

  /** Resets all state except for coord list.
    * INTERNAL API.
    */
  def reset(): Unit = {
    datasetInfo.values.foreach(_.metrics.shutdown())
    datasetInfo.clear()
    shardMappers.clear()
    subscriptions = subscriptions.clear
  }

  /** Here the origin can be a client, forwarded from the `NodeClusterActor`.
    * The response is returned directly to the requester.
    */
  def sendSnapshot(ref: DatasetRef, origin: ActorRef): Unit =
    origin ! mapperOpt(ref).getOrElse(DatasetUnknown(ref))

  private def mapperOpt(ref: DatasetRef): Option[CurrentShardSnapshot] =
    shardMappers.get(ref).map(m => CurrentShardSnapshot(ref, m))

  /** Selects the `ShardMapper` for the provided dataset, updates the mapper
    * for the received shard event from the event source.
    */
  def updateFromShardEventNoPublish(e: ShardEvent): Unit =
    shardMappers.get(e.ref) foreach (m => m.updateFromEvent(e))

  def addMember(coord: ActorRef, memberAddr: Address): Unit = {
    logger.info(s"Initiated addMember for Coord $coord")
    coords += coord
    for {
      (dataset, state) <- datasetInfo
      mapper = shardMappers(dataset)
      resources = state.resources
    } {
      val assignments = strategy.shardAssignments(coord, dataset, resources, mapper)
      if (!assignments.isEmpty) runShardAssignmentProtocol(dataset, coord, assignments)
    }
    logger.info(s"Completed addMember for Coord $coord")
  }

  def removeMember(coord: ActorRef): Unit = {
    logger.info(s"Initiated removeMember for Coord $coord")
    coords -= coord
    for {
      (dataset, state) <- datasetInfo
      mapper = shardMappers(dataset)
      resources = state.resources
    } {
      runShardUnassignmentProtocol(dataset, coord, mapper)
      assignShardsToNodes(dataset, mapper, resources)
    }
    logger.info(s"Completed removeMember for Coord $coord")
  }

  /**
    * Adds new dataset to cluster, thereby initiating new shard assignments to existing nodes
    * @return new assignments that were made. Empty if dataset already exists.
    */
  def addDataset(setup: SetupDataset,
                 dataset: Dataset,
                 ackTo: ActorRef): Map[ActorRef, Seq[Int]] = {

    logger.info(s"Initiated Setup for Dataset ${setup.ref}")
    mapperOpt(setup.ref) match {
      case Some(_) =>
        logger.info(s"Dataset ${setup.ref} already exists - ignoring")
        ackTo ! DatasetExists(setup.ref)
        Map.empty
      case None =>
        val mapper = new ShardMapper(setup.resources.numShards)
        val resources = setup.resources
        val metrics = new ShardHealthStats(setup.ref, mapper)
        val source = setup.source
        val state = DatasetInfo(resources, metrics, source, dataset)
        datasetInfo(dataset.ref) = state
        shardMappers(dataset.ref) = mapper

        val assignments = assignShardsToNodes(dataset.ref, mapper, resources)

        subscriptions :+= ShardSubscription(dataset.ref, Set.empty)
        subscriptions.watchers foreach (subscribe(_, dataset.ref))
        subscriptions.watchers foreach (_ ! CurrentShardSnapshot(dataset.ref, mapper))
        logger.info(s"Completed Setup for Dataset ${setup.ref}")
        ackTo ! DatasetVerified
        assignments
    }
  }

  private def assignShardsToNodes(dataset: DatasetRef,
                                  mapper: ShardMapper,
                                  resources: DatasetResourceSpec): Map[ActorRef, Seq[Int]] = {
    val assignments = for {
      coord <- latestCoords // assign shards on newer nodes first
    } yield {
      val assignments = strategy.shardAssignments(coord, dataset, resources, mapper)
      if (!assignments.isEmpty) runShardAssignmentProtocol(dataset, coord, assignments)
      coord -> assignments
    }
    assignments.toMap
  }

  def removeDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Initiated removal for Dataset $dataset")
    for {
      coord <- coords
      mapper = shardMappers(dataset)
    } runShardUnassignmentProtocol(dataset, coord, mapper)
    datasetInfo remove dataset
    shardMappers remove dataset
    subscriptions = subscriptions - dataset
    logger.info(s"Completed removal for Dataset $dataset")
  }

  /**
    * Intended for recovery of ShardMapper state only - recovers a current ShardMap as well as updating a list of
    * members / coordinator ActorRefs
    */
  def recoverShardState(ref: DatasetRef, map: ShardMapper): Unit = {
    logger.info(s"!!! Recovering map for dataset ${ref}")
    shardMappers(ref) = map
    logger.debug(s"Map contents: ${map}")
  }

  def recoverSubscriptions(subs: ShardSubscriptions): Unit = {
    logger.info(s"!!! Recovering (adding) subscriptions from $subs")
    subscriptions = subscriptions.copy(subscriptions = subscriptions.subscriptions ++ subs.subscriptions,
      watchers = subscriptions.watchers ++ subs.watchers)
    logger.debug(s"Final subscriptions = $subscriptions")
  }

  /** Selects the `ShardMapper` for the provided dataset, updates the mapper
    * for the received shard event from the event source, and publishes
    * the event to all subscribers of that event and dataset.
    */
  def updateFromShardEventAndPublish(event: ShardEvent): Unit =
    shardMappers.get(event.ref) foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"Invalid shard.", l)
        case Success(r) =>
          logger.info(s"Updated mapper for dataset ${event.ref} event $event")
      }
      // TODO if failure we don't need to publish, though that's what we have
      // been doing thus far. This requires changing tests out of scope for the current changes
      publishEvent(event)
    }

  private def runShardAssignmentProtocol(dataset: DatasetRef,
                                         coord: ActorRef,
                                         shards: Seq[Int]): Unit = {
    val state = datasetInfo(dataset)
    sendDatasetSetup(coord, state.dataset, state.source)
    for { shard <- shards } {
      val event = ShardAssignmentStarted(dataset, shard, coord)
      updateFromShardEventAndPublish(event)
    }
    sendStartCommands(coord, dataset, shards)
  }

  /** Called on successful AddNodeCoordinator and SetupDataset protocols.
    *
    * @param coord the current cluster members
    * @param dataset the Dataset object
    * @param source the ingestion source type to use
    */
  private def sendDatasetSetup(coord: ActorRef, dataset: Dataset, source: IngestionSource): Unit = {
    logger.info(s"Sending setup message for ${dataset.ref} to coordinators $coord.")
    val setupMsg = IngestionCommands.DatasetSetup(dataset.asCompactString, source)
    coord ! setupMsg
  }

  /** If no shards are assigned to a coordinator, no commands are sent. */
  private def sendStartCommands(coord: ActorRef, ref: DatasetRef, shards: Seq[Int]): Unit = {
    logger.info(s"Sending start ingestion message for ${ref} to coordinators $coord.")
    for {shard <- shards} coord ! StartShardIngestion(ref, shard, None)
  }

  private def runShardUnassignmentProtocol(dataset: DatasetRef, coord: ActorRef, mapper: ShardMapper): Unit = {
    val shardsToDown = mapper.shardsForCoord(coord)
    for { shard <- shardsToDown } {
      val event = ShardDown(dataset, shard, coord)
      updateFromShardEventAndPublish(event)
      // note member may be down already, and this message may never be delivered
      coord ! StopShardIngestion(dataset, shard)
    }
  }

  /** Publishes the event to all subscribers of that dataset. */
  private def publishEvent(e: ShardEvent): Unit =
    for {
      subscription <- subscriptions.subscription(e.ref)
    } subscription.subscribers foreach (_ ! e)

  private def latestCoords: Seq[ActorRef] =
    coords.foldLeft(List[ActorRef]())((x, y) => y :: x) // reverses the set

}
