package filodb.coordinator

import scala.collection.mutable
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, Address, AddressFromURIString}
import com.typesafe.scalalogging.StrictLogging
import org.scalactic._

import filodb.coordinator.NodeClusterActor._
import filodb.core.{DatasetRef, ErrorResponse, Response, Success => SuccessResponse}
import filodb.core.downsample.DownsampleConfig
import filodb.core.metadata.Dataset
import filodb.core.store.{AssignShardConfig, IngestionConfig, StoreConfig}

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
private[coordinator] final class ShardManager(settings: FilodbSettings,
                                              strategy: ShardAssignmentStrategy) extends StrictLogging {

  import ShardManager._

  private var _subscriptions = ShardSubscriptions(Set.empty, Set.empty)
  private val _datasetInfo = new mutable.HashMap[DatasetRef, DatasetInfo]
  private val _shardMappers = new mutable.HashMap[DatasetRef, ShardMapper]
  // preserve deployment order - newest last
  private val _coordinators = new mutable.LinkedHashMap[Address, ActorRef]
  private val _errorShardReassignedAt = new mutable.HashMap[DatasetRef, mutable.HashMap[Int, Long]]

  // TODO move to startup-v2
  private val _tenantIngestionMeteringOpt =
    if (settings.config.getBoolean("shard-key-level-ingestion-metrics-enabled")) {
      val inst = TenantIngestionMetering(
                   settings,
                   () => { _datasetInfo.map{ case (dsRef, _) => dsRef}.toIterator },
                   () => { _coordinators.head._2 })
      inst.schedulePeriodicPublishJob()
      Some(inst)
    } else None

  val shardReassignmentMinInterval = settings.config.getDuration("shard-manager.reassignment-min-interval")

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

  def logAllMappers(msg: String = ""): Unit = {
    _shardMappers.foreach { case (ref, mapper) =>
      logger.info(s"$msg dataset=$ref Current mapper state:\n${mapper.prettyPrint}")
    }
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
    mapperCopyOpt(dataset) match {
      case Some(current) =>
        logger.info(s"Adding $subscriber as a subscriber for dataset=$dataset")
        _subscriptions = subscriptions.subscribe(subscriber, dataset)
        _subscriptions.watchers foreach (_ ! subscriptions)
        subscriber ! current
      case _ =>
        logger.error(s"dataset=$dataset unknown, unable to subscribe $subscriber.")
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
    origin ! mapperCopyOpt(ref).getOrElse(DatasetUnknown(ref))

  /**
    * Returns a complete copy of the ShardMapper within a CurrentShardSnapshot, if the dataset
    * exists. Although a copy of the ShardMapper isn't typically required, it is required for
    * the tests to work properly. This is because the TestProbe provides access to the local
    * ShardMapper instance, and so any observation of the snapshot would expose the latest
    * mappings instead. The complete copy also offers a nice safeguard, in case the ShardMapper
    * is concurrently modified before the message is sent. This isn't really expected, however.
    */
  private def mapperCopyOpt(ref: DatasetRef): Option[CurrentShardSnapshot] =
    _shardMappers.get(ref).map(m => CurrentShardSnapshot(ref, m.copy()))

  /**
    * Same as mapperCopyOpt, except it directly references the ShardMapper instance.
    */
  private def mapperOpt(ref: DatasetRef): Option[CurrentShardSnapshot] =
    _shardMappers.get(ref).map(m => CurrentShardSnapshot(ref, m))

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
      if (assignable.nonEmpty) {
        doAssignShards(dataset, coordinator, assignable)
        publishChanges(dataset)
      }
    }
    logAllMappers(s"Completed addMember for coordinator $coordinator. Status Map:")
  }

  /** Called on MemberRemoved, new status already updated. */
  def removeMember(address: Address): Option[ActorRef] = {
    _coordinators.get(address) map { coordinator =>
      logger.info(s"Initiated removeMember for coordinator=$coordinator on $address")
      _coordinators remove address
      removeCoordinator(coordinator)
      logAllMappers(s"Completed removeMember for coordinator $address")
      coordinator
    }
  }

  private def updateShardMetrics(): Unit = {
    _datasetInfo.foreach { case (dataset, info) =>
        info.metrics.update(_shardMappers(dataset))
    }
  }

  import OptionSugar._

  /**
    * Validate whether the dataset exists.
    *
    * @param dataset - Input dataset
    * @return - shardMapper for the dataset
    */
  def validateDataset(dataset: DatasetRef): ShardMapper Or ErrorResponse = {
    _shardMappers.get(dataset).toOr(DatasetUnknown(dataset))
  }

  /**
    * Validate whether the given node exists or not.
    *
    * @param address - Node address
    * @param shards - List of shards
    * @return - coordinator for the node address
    */
  def validateCoordinator(address: String, shards: Seq[Int]): ActorRef Or ErrorResponse = {
    _coordinators.get(AddressFromURIString(address)).toOr(BadData(s"$address not found"))
  }

  /**
    * Check if all the given shards are valid:
    *  i. Shard number should be >= 0 and < maxAllowedShard
    *  ii. Shard should not be already assigned to given node in ReassignShards request
    *  iii. Shard should be not be already assigned to any node
    *
    * @param shards - List of shards
    * @param shardMapper - ShardMapper object
    * @param coord - Coordinator
    * @return - The list of valid shards
    */
  def validateShards(shards: Seq[Int], shardMapper: ShardMapper, coord: ActorRef): Seq[Int] Or ErrorResponse = {
    val validShards: Seq[Int] = shards.filter(shard => shard >= 0 && shard < shardMapper.numShards).distinct
    if (validShards.isEmpty || validShards.size != shards.size) {
      Bad(BadSchema(s"Invalid shards found $shards. Valid shards are $validShards"))
    } else if (validShards.exists(shard => shardMapper.coordForShard(shard) == coord)) {
      Bad(BadSchema(s"Can not reassign shards to same node: $shards"))
    } else if (validShards.exists(shard => shardMapper.coordForShard(shard) != ActorRef.noSender)) {
      Bad(BadSchema(s"Can not start $shards on $coord. Please stop shards before starting"))
    } else Good(validShards)
  }

  /**
    * Check if all the given shards are valid:
    *     - Shard number should be >= 0 and < maxAllowedShard
    *     - Shard should be already assigned to one node
    * @param shards - List of shards to be stopped
    * @param shardMapper - Shard Mapper object
    * @return
    */
  def validateShardsToStop(shards: Seq[Int], shardMapper: ShardMapper): Seq[Int] Or ErrorResponse = {
    val validShards: Seq[Int] = shards.filter(shard => shard >= 0 && shard < shardMapper.numShards).distinct
    if (validShards.isEmpty || validShards.size != shards.size) {
      Bad(BadSchema(s"Invalid shards found $shards. Valid shards are $validShards"))
    } else if (validShards.exists(shard => shardMapper.coordForShard(shard) == ActorRef.noSender)) {
      Bad(BadSchema(s"Can not stop shards $shards not assigned to any node"))
    } else Good(validShards)
  }

  /**
    * Verify whether there are enough capacity to add new shards on the node.
    * Using ShardAssignmentStrategy get the remaining capacity of the node.
    * Validate the same against shard list in the shardStop request.
    *
    * @param shardList - List of shards
    * @param shardMapper - ShardMapper object
    * @param dataset - Dataset fromthe request
    * @param resources - Dataset resources
    * @param coord - Coordinator
    * @return - Bad/Good
    */
  def validateNodeCapacity(shardList: Seq[Int], shardMapper: ShardMapper, dataset: DatasetRef,
                           resources: DatasetResourceSpec, coord: ActorRef): Unit Or ErrorResponse = {
    val shardMapperNew = shardMapper.copy() // This copy is done to simulate the assignmentStrategy
    shardList.foreach(shard => shardMapperNew.updateFromEvent(
      ShardDown(dataset, shard, shardMapperNew.coordForShard(shard))))
    val assignable = strategy.remainingCapacity(coord, dataset, resources, shardMapperNew)
    if (assignable <= 0 && shardList.size > assignable) {
      Bad(BadSchema(s"Capacity exceeded. Cannot allocate more shards to $coord"))
    } else Good(())
  }

  /**
    * Stop the required shards against the given dataset.
    * Returns DatasetUnknown for dataset that does not exist.
    */
  def stopShards(shardStopReq: StopShards, ackTo: ActorRef): Unit = {
    logger.info(s"Stop Shard request=${shardStopReq.unassignmentConfig} " +
                  s"for dataset=${shardStopReq.datasetRef} ")
    val answer: Response = validateRequestAndStopShards(shardStopReq, ackTo)
                            .fold(_ => SuccessResponse, errorResponse => errorResponse)
    logAllMappers(s"Completed stopShards $shardStopReq")
    ackTo ! answer
  }

  /**
    * Validates the given stopShard request.
    * Stops shard from current active node.
    *
    * Performs the validations serially.
    *
    * @return - Validates and returns error message on failure and a unit if no validation error
    */
  private def validateRequestAndStopShards(shardStopReq: StopShards, ackTo: ActorRef): Unit Or ErrorResponse = {
    for {
      shardMapper <- validateDataset(shardStopReq.datasetRef)
      shards      <- validateShardsToStop(shardStopReq.unassignmentConfig.shardList, shardMapper)
    } yield {
      unassignShards(shards, shardStopReq.datasetRef, shardMapper)
    }
  }

  /**
    * Shutdown shards from the coordinator where it is running
    */
  private def unassignShards(shards: Seq[Int],
                             dataset: DatasetRef,
                             shardMapper: ShardMapper): Unit = {
    for { shard <-  shards} {
      val curCoordinator = shardMapper.coordForShard(shard)
      doUnassignShards(dataset, curCoordinator, Seq(shard))
    }
    publishChanges(dataset)
  }

  /**
    * Start the shards on the given coordinator.
    * Returns DatasetUnknown for dataset that does not exist.
    */
  def startShards(shardStartReq: StartShards, ackTo: ActorRef): Unit = {
    logger.info(s"Start Shard request=${shardStartReq.assignmentConfig} " +
                  s"for dataset=${shardStartReq.datasetRef} ")
    val answer: Response = validateRequestAndStartShards(shardStartReq.datasetRef,
                                                         shardStartReq.assignmentConfig, ackTo)
                              .fold(_ => SuccessResponse, errorResponse => errorResponse)
    logAllMappers(s"Completed startShards $shardStartReq")
    ackTo ! answer
  }

  /**
    * Validates the start shard request.
    * Starts shards on the new node only if valid.
    *
    * Performs the validations serially.
    *
    * @return - Validates and returns error message on failure and a unit if no validation error
    */
  private def validateRequestAndStartShards(dataset: DatasetRef,
                                            assignmentConfig: AssignShardConfig,
                                            ackTo: ActorRef): Unit Or ErrorResponse = {
    for {
      shardMapper <- validateDataset(dataset)
      coordinator <- validateCoordinator(assignmentConfig.address, assignmentConfig.shardList)
      shards      <- validateShards(assignmentConfig.shardList, shardMapper, coordinator)
      _           <- validateNodeCapacity(shards, shardMapper, dataset,
                                          _datasetInfo(dataset).resources, coordinator)
    } yield {
      doAssignShards(dataset, coordinator, shards)
      publishChanges(dataset)
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
      val toRemove = allRegisteredNodes -- coordinators // coordinators is the list of recovered nodes
      logger.info(s"Cleaning up dataset=$dataset stale coordinators $toRemove after recovery")
      toRemove
    }
    for { coord <- nodesToRemove.flatten } {
      removeCoordinator(coord)
    }
    updateShardMetrics()
    logAllMappers("Finished removing stale coordinators")
  }

  private def removeCoordinator(coordinator: ActorRef): Unit = {
    for ((dataset, resources, mapper) <- datasetShardMaps) {
      var shardsToDown = mapper.shardsForCoord(coordinator)
      doUnassignShards(dataset, coordinator, shardsToDown)
      // try to reassign shards that were unassigned to other nodes that have room.
      assignShardsToNodes(dataset, mapper, resources)
      publishChanges(dataset)
    }
  }

  /**
    * Adds new dataset to cluster, thereby initiating new shard assignments to existing nodes
    * @return new assignments that were made. Empty if dataset already exists.
    */
  def addDataset(dataset: Dataset,
                 ingestConfig: IngestionConfig,
                 source: IngestionSource,
                 ackTo: Option[ActorRef]): Map[ActorRef, Seq[Int]] = {
    logger.info(s"Initiated setup for dataset=${dataset.ref}")
    val answer: Map[ActorRef, Seq[Int]] = mapperOpt(dataset.ref) match {
      case Some(_) =>
        logger.info(s"dataset=${dataset.ref} already exists - skipping addDataset workflow")
        ackTo.foreach(_ ! DatasetExists(dataset.ref))
        Map.empty
      case None =>
        val resources = DatasetResourceSpec(ingestConfig.numShards, ingestConfig.minNumNodes)
        val mapper = new ShardMapper(resources.numShards)
        _shardMappers(dataset.ref) = mapper
        // Access the shardmapper through the HashMap so even if it gets replaced it will update the shard stats
        val metrics = new ShardHealthStats(dataset.ref)
        val state = DatasetInfo(resources, metrics, source, ingestConfig.downsampleConfig,
                                ingestConfig.storeConfig, dataset)
        _datasetInfo(dataset.ref) = state

        // NOTE: no snapshots get published here because nobody subscribed to this dataset yet
        val assignments = assignShardsToNodes(dataset.ref, mapper, resources)

        // Add dataset to subscribers and send initial ShardMapper snapshot
        _subscriptions :+= ShardSubscription(dataset.ref, Set.empty)
        _subscriptions.watchers foreach (subscribe(_, dataset.ref))
        logAllMappers(s"Completed setup for dataset=${dataset.ref}")
        ackTo.foreach(_ ! DatasetVerified)
        assignments
    }
    publishChanges(dataset.ref)
    answer
  }

  private def assignShardsToNodes(dataset: DatasetRef,
                                  mapper: ShardMapper,
                                  resources: DatasetResourceSpec,
                                  excludeCoords: Seq[ActorRef] = Nil): Map[ActorRef, Seq[Int]] = {
    (for {
      coord <- latestCoords if !excludeCoords.contains(coord) // assign shards on newer nodes first
    } yield {
      val assignable = strategy.shardAssignments(coord, dataset, resources, mapper)
      if (assignable.nonEmpty) doAssignShards(dataset, coord, assignable)
      coord -> assignable
    }).toMap
  }

  def removeDataset(dataset: DatasetRef): Unit = {
    logger.info(s"Initiated removal for dataset=$dataset")
    for {
      (_, coord) <- _coordinators
      mapper = _shardMappers(dataset)
      shardsToDown = mapper.shardsForCoord(coord)
    } doUnassignShards(dataset, coord, shardsToDown)
    publishChanges(dataset)
    _datasetInfo remove dataset
    _shardMappers remove dataset
    _subscriptions = _subscriptions - dataset
    logAllMappers(s"Completed removal of dataset=$dataset")
  }

  /**
    * Intended for recovery of ShardMapper state only - recovers a current ShardMap as well as updating a list of
    * members / coordinator ActorRefs
    */
  def recoverShards(ref: DatasetRef, map: ShardMapper): Unit = {
    logger.info(s"Recovering ShardMap for dataset=$ref ; ShardMap contents: $map")
    _shardMappers(ref) = map
    publishChanges(ref)
  }

  def recoverSubscriptions(subs: ShardSubscriptions): Unit = {
    logger.info(s"Recovering (adding) subscriptions from $subs")
    // we have to remove existing subscriptions (which are probably empty) for datasets, otherwise new ones
    // might not take hold
    val newSubRefs = subs.subscriptions.map(_.dataset)
    val trimmedSubs = _subscriptions.subscriptions.filterNot(newSubRefs contains _.dataset)
    _subscriptions = subscriptions.copy(subscriptions = trimmedSubs ++ subs.subscriptions,
      watchers = _subscriptions.watchers ++ subs.watchers)
    logger.debug(s"Recovered subscriptions = $subscriptions")
  }

  /**
    * Applies an event (usually) from IngestionActor to
    * the ShardMapper for dataset.
    */
  def updateFromExternalShardEvent(sender: ActorRef, event: ShardEvent): Unit = {
    _shardMappers.get(event.ref) foreach { mapper =>
      val currentCoord = mapper.coordForShard(event.shard)
      if (currentCoord == ActorRef.noSender) {
        logger.debug(s"Ignoring event=$event from sender=$sender for dataset=${event.ref} since shard=${event.shard} " +
          s"is not currently assigned. Was $sender the previous owner for a shard that was just unassigned? " +
          s"How else could this happen? ")
        // Note that this path is not used for an initial shard assignment when currentCoord would indeed be noSender;
        // used only for reacting to shard events sent from member nodes.
      } else if (currentCoord.path.address == sender.path.address) {
        // Above condition ensures that we respond to shard events only from the node shard is currently assigned to.
        // Needed to avoid race conditions where IngestionStopped for an old assignment comes after shard is reassigned.
        updateFromShardEvent(event)
        logAllMappers(s"After Update from event $event")
        // reassign shard if IngestionError. Exclude previous node since it had error shards.
        event match {
          case _: IngestionError =>
            require(mapper.unassignedShards.contains(event.shard))
            val lastReassignment = getShardReassignmentTime(event.ref, event.shard)
            val now = System.currentTimeMillis()
            val info = _datasetInfo(event.ref)
            if (now - lastReassignment > shardReassignmentMinInterval.toMillis) {
              logger.warn(s"Attempting to reassign shard=${event.shard} from dataset=${event.ref}. " +
                s"It was last reassigned at $lastReassignment")
              val assignments = assignShardsToNodes(event.ref, mapper, info.resources, Seq(currentCoord))
              if (assignments.valuesIterator.flatten.contains(event.shard)) {
                setShardReassignmentTime(event.ref, event.shard, now)
                info.metrics.numErrorReassignmentsDone.increment()
                logAllMappers(s"Successfully reassigned dataset=${event.ref} shard=${event.shard}")
              } else {
                info.metrics.numErrorReassignmentsSkipped.increment()
                logAllMappers(s"Could not reassign dataset=${event.ref} shard=${event.shard}")
                logger.warn(s"Shard=${event.shard} from dataset=${event.ref} was NOT reassigned possibly " +
                  s"because no other node was available")
              }
            } else {
              info.metrics.numErrorReassignmentsSkipped.increment()
              logger.warn(s"Skipping reassignment of shard=${event.shard} from dataset=${event.ref} since " +
                s"it was already reassigned within ${shardReassignmentMinInterval} at ${lastReassignment}")
            }
          case _ =>
        }
        // RecoveryInProgress status results in too many messages that really do not need a publish
        if (!event.isInstanceOf[RecoveryInProgress]) publishSnapshot(event.ref)
      } else {
        logger.debug(s"Ignoring event $event from $sender for dataset=${event.ref} since it does not match current " +
          s"owner of shard=${event.shard} which is ${mapper.coordForShard(event.shard)}")
      }
    }
  }

  private def getShardReassignmentTime(dataset: DatasetRef, shard: Int): Long = {
    val shardReassignmentMap = _errorShardReassignedAt.getOrElseUpdate(dataset, mutable.HashMap())
    shardReassignmentMap.getOrElse(shard, 0L)
  }

  private def setShardReassignmentTime(dataset: DatasetRef, shard: Int, time: Long): Unit = {
    val shardReassignmentMap = _errorShardReassignedAt.getOrElseUpdate(dataset, mutable.HashMap())
    shardReassignmentMap(shard) = time
  }

  /** Selects the `ShardMapper` for the provided dataset and updates the mapper
    * for the received shard event from the event source
    */
  private def updateFromShardEvent(event: ShardEvent): Unit = {
    _shardMappers.get(event.ref) foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"updateFromShardEvent error for dataset=${event.ref} event $event. Mapper now: $mapper", l)
        case Success(r) =>
          logger.debug(s"updateFromShardEvent success for dataset=${event.ref} event $event. Mapper now: $mapper")
      }
    }
    updateShardMetrics()
  }

  private def doAssignShards(dataset: DatasetRef,
                             coord: ActorRef,
                             shards: Seq[Int]): Unit = {
    logger.info(s"Assigning shards for dataset=$dataset to " +
      s"coordinator $coord for shards $shards")
    for { shard <- shards }  {
      val event = ShardAssignmentStarted(dataset, shard, coord)
      updateFromShardEvent(event)
    }
  }

  private def doUnassignShards(dataset: DatasetRef,
                               coordinator: ActorRef,
                               shardsToDown: Seq[Int]): Unit = {
    logger.info(s"Unassigning shards for dataset=$dataset to " +
      s"coordinator $coordinator for shards $shardsToDown")
    for { shard <- shardsToDown } {
      val event = ShardDown(dataset, shard, coordinator)
      updateFromShardEvent(event)
    }
  }

  /**
    * To be called after making a bunch of changes to the ShardMapper for the given dataset.
    * Calling this method more often is permitted, but it generates more publish messages
    * than is necessary.
    */
  private def publishChanges(ref: DatasetRef): Unit = {
    publishSnapshot(ref)
    updateShardMetrics()
  }

  /** Publishes a ShardMapper snapshot of given dataset to all subscribers of that dataset. */
  def publishSnapshot(ref: DatasetRef): Unit = {
    mapperCopyOpt(ref) match {
      case Some(snapshot) => {
        for {
          subscription <- _subscriptions.subscription(ref)
        } subscription.subscribers foreach (_ ! snapshot)

        // Also send a complete ingestion state command to all ingestion actors. Without this,
        // they won't start or stop ingestion.
        // Note that all coordinators also get latest snapshot through this.

        // TODO: Need to provide a globally consistent version, incremented when anything
        //       changes, for any dataset.
        val resync = ShardIngestionState(0, snapshot.ref, snapshot.map)

        for (coord <- coordinators) {
          coord ! resync
        }
      }
      case None =>
        logger.warn(s"Cannot publish snapshot which doesn't exist for ref $ref")
    }
  }

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
                               downsample: DownsampleConfig,
                               storeConfig: StoreConfig,
                               dataset: Dataset)
}
