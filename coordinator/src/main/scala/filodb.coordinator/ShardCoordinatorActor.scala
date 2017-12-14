package filodb.coordinator

import scala.collection.mutable.{HashMap => MutableHashMap, HashSet => MutableHashSet}
import scala.util.{Failure, Success}

import akka.actor._
import akka.event.LoggingReceive

import filodb.coordinator.NodeClusterActor.{IngestionSource, SetupDataset}
import filodb.core.DatasetRef
import filodb.core.metadata.Dataset

/** This actor manages the following for its parent, the cluster singleton,
  * [[filodb.coordinator.NodeClusterActor]]:
  *  1. All [[filodb.coordinator.ShardMapper]]s
  *  2. Subscriptions for dataset shard state events
  *  3. Current Subscribers for shard state events via Deathwatch
  *  4. Publishes ShardEvents to subscribers of the shard event's dataset
  *
  * Some subscribers are node coordinator actors, their deathwatch also is done
  * here in order to update the shard node status directly.
  */
private[coordinator] final class ShardCoordinatorActor(strategy: ShardAssignmentStrategy) extends NamingAwareBaseActor {

  import NodeClusterActor.DatasetUnknown
  import ShardAssignmentStrategy.{DatasetShards, DatasetResources}
  import ShardSubscriptions._

  val shardMappers = new MutableHashMap[DatasetRef, ShardMapper]
  val members      = new MutableHashSet[ActorRef]
  val resources    = new MutableHashMap[DatasetRef, DatasetResources]
  val metrics      = new MutableHashMap[DatasetRef, ShardHealthStats]

  var subscriptions = ShardSubscriptions.Empty

  def sharding: Actor.Receive = LoggingReceive {
    case e: ShardEvent           => publish(e)
    case e: AddDataset           => addDataset(e, sender())
    case e: AddMember            => addMember(e, sender())
    case RemoveDataset(ds)       => removeDataset(ds)
    case RemoveMember(ref)       => removeMember(ref, Some(sender()))
    case r: RecoverShardState    => recoverShardState(r)
    case NodeProtocol.ResetState => reset(sender())
  }

  def subscribers: Actor.Receive = LoggingReceive {
    case e: Subscribe            => subscribe(e)
    case SubscribeAll            => subscribeAll(sender())
    case Unsubscribe(actor)      => unsubscribe(actor)
    case Terminated(actor)       => unsubscribe(actor)
    case RecoverSubscriptions(subs) => recoverSubscriptions(subs)
  }

  def reads: Actor.Receive = LoggingReceive {
    case GetSubscribers(ds)      => sendSubscribers(ds, sender())
    case GetSubscriptions        => sendSubscriptions(sender())
    case GetSnapshot(ref)        => snapshot(ref, sender())
  }

  override def receive: Actor.Receive = sharding orElse subscribers orElse reads

  /**
   * Intended for recovery of ShardMapper state only - recovers a current ShardMap as well as updating a list of
   * members / coordinator ActorRefs
   */
  private def recoverShardState(r: RecoverShardState): Unit = {
    logger.info(s"!!! Recovering map for dataset ${r.ref}")
    shardMappers(r.ref) = r.map
    members ++= r.map.allNodes
    logger.debug(s"Map contents: ${r.map}   Members are now:  $members")
  }

  private def recoverSubscriptions(subs: ShardSubscriptions): Unit = {
    logger.info(s"!!! Recovering (adding) subscriptions from $subs")
    subscriptions = subscriptions.copy(subscriptions = subscriptions.subscriptions ++ subs.subscriptions,
                                       watchers = subscriptions.watchers ++ subs.watchers)
    logger.debug(s"Final subscriptions = $subscriptions")
  }

  /** Selects the `ShardMapper` for the provided dataset, updates the mapper
    * for the received shard event from the event source, and publishes
    * the event to all subscribers of that event and dataset.
    */
  private def publish(e: ShardEvent): Unit =
    shardMappers.get(e.ref) foreach (update(e, _))

  /** Updates the local `ShardMapper`'s `ShardStatus`, and publishes to the
    * appropriate subscribers to similarly sync their shard status.
    * If assigning a shard, registers shard for node and updates status to assigning.
    * If unassigning a shard, removes shard from node and updates status to unassigning.
    */
  private def update(event: ShardEvent, map: ShardMapper): Unit = {
    map.updateFromEvent(event) match {
      case Failure(l) =>
        logger.error(s"Invalid shard.", l)
      case Success(r) =>
        shardMappers(event.ref) = map
    }
    // TODO if failure we don't need to publish, though that's what we have
    // been doing thus far. This requires changing tests out of scope for the current changes
    publishEvent(event)
  }

  /** Publishes the event to all subscribers of that dataset. */
  private def publishEvent(e: ShardEvent): Unit =
    for {
      subscription <- subscriptions.subscription(e.ref)
    } subscription.subscribers foreach (_ ! e)

  /** Sent from the [[filodb.coordinator.NodeClusterActor]] on SetupDataset.
    * If the dataset subscription exists, returns a DatasetExists to the cluster
    * actor, otherwise adds the new dataset (subscription) via the shard
    * assignment strategy. Subscribes all known [[akka.cluster.ClusterEvent.MemberUp]]
    * members in the ring to the new subscription. Sends the cluster actor a
    * `SubscriptionAdded` to proceed in the dataset's setup. Sends the commands
    * from the assignment strategy to the provided member coordinators.
    *
    * Subscribes the `NodeGuardian` to the new subscription.
    *
    * INTERNAL API. Idempotent.
    */
  private def addDataset(e: AddDataset, origin: ActorRef): Unit =
    mapperOpt(e.setup.ref) match {
      case Some(exists) =>
        e.ackTo ! NodeClusterActor.DatasetExists(e.setup.ref)

      case _ =>
        val added = strategy.datasetAdded(e.setup.ref, e.coordinators, e.setup.resources, shardMappers)
        for {
          (node, shards) <- added.shards
          shard          <- shards
        } update(ShardAssignmentStarted(added.ref, shard, node), added.mapper)

        // no MemberUp yet, new mapper, no shards yet, added.shards was empty
        if (e.coordinators.isEmpty && added.shards.isEmpty) {
          shardMappers(added.ref) = added.mapper
        }

        metrics(added.ref) = new ShardHealthStats(added.ref, shardMappers(added.ref))
        subscriptions :+= ShardSubscription(added.ref, Set.empty)
        subscriptions.watchers foreach (subscribe(_, added.ref))
        subscriptions.watchers foreach (_ ! CurrentShardSnapshot(added.ref, added.mapper))
        resources(added.ref) = added.resources
        logger.info(s"Dataset '${added.ref}' added, created new ${added.mapper}")

        origin ! DatasetAdded(e.dataset, e.setup.source, added.shards, e.ackTo)
    }

  /** Shard assignment strategy adds the new node and returns the `ShardsAssigned`.
    * The set up local `ShardMapper`s are updated. Sends `CoordinatorAdded`
    * to initiate [[filodb.coordinator.NodeClusterActor.sendDatasetSetup]]
    * and start ingestion on any added shards.
    *
    * Locally registers newly-assigned shards to node and updates status in the `ShardMapper`
    * then publishes `ShardAssignmentStarted` for each shard to subscribers to
    * sync the status, allowing users know that an assignment is in progress.
    *
    * INTERNAL API.
    */
  private def addMember(e: AddMember, origin: ActorRef): Unit =
    if (!members.contains(e.coordinator)) {
      val added = strategy.nodeAdded(e.coordinator, shardMappers, resources)
      logger.info(s"Added new ${e.coordinator}")
      members += e.coordinator
      for {
        DatasetShards(ds, map, shards) <- added.shards
        shard <- shards
      } update(ShardAssignmentStarted(ds, shard, e.coordinator), map)

      origin ! CoordinatorAdded(e.coordinator, added.shards, e.addr)
    } else {
      logger.info(s"Member $e was already added, skipping")
      origin ! CoordinatorAdded(e.coordinator, Nil, e.addr)
    }

  /** Removes a Node coordinator/member, execute any resulting commands, update maps.
    * Publishes a `ShardMemberRemoved` to the appropriate subscribers which will
    * update their local shards for the removed coordinator as unassigned.
    */
  private def removeMember(coordinator: ActorRef, origin: Option[ActorRef]): Unit =
    if (members contains coordinator) {
      val removed = strategy.nodeRemoved(coordinator, shardMappers)
      logger.info(s"Removed $coordinator")
      members -= coordinator
      for {
        DatasetShards(ds, map, shards) <- removed.shards
        shard <- shards
      } update(ShardMemberRemoved(ds, shard, coordinator), map)

      origin foreach (_ ! CoordinatorRemoved(coordinator, removed.shards))
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
  private def subscribe(e: Subscribe): Unit =
    mapperOpt(e.dataset) match {
      case Some(current) =>
        logger.info(s"Adding ${e.subscriber} as a subscriber for dataset ${e.dataset}")
        subscribe(e.subscriber, e.dataset)
        e.subscriber ! current
      case _ =>
        logger.error(s"Dataset ${e.dataset} unknown, unable to subscribe ${e.subscriber}.")
        e.subscriber ! DatasetUnknown(e.dataset)
    }

  /** Here the origin can be a client, forwarded from the `NodeClusterActor`.
    * The response is returned directly to the requester.
    */
  private def snapshot(ref: DatasetRef, origin: ActorRef): Unit =
    origin ! mapperOpt(ref).getOrElse(DatasetUnknown(ref))

  private def mapperOpt(ref: DatasetRef): Option[CurrentShardSnapshot] =
    shardMappers.get(ref).map(m => CurrentShardSnapshot(ref, m))

  /** Subscribes the internal actor to shard events and sends current
    * snapshot of subscribers per dataset. This `subsce`
    */
  private def subscribeAll(subscriber: ActorRef): Unit = {
    logger.info(s"Subscribing $subscriber to events from all datasets and subscriptions")
    subscriptions = subscriptions subscribe subscriber
    subscriptions.watchers foreach (sendSubscriptions(_))
    context watch subscriber
    // send the subscriber all current shardMappers
    shardMappers foreach { case (ref, map) => subscriber ! CurrentShardSnapshot(ref, map) }
  }

  /** Subscribes a subscriber to an existing dataset's shard updates.
    * Idempotent. Sends watchers the updated subscriptions.
    */
  private def subscribe(subscriber: ActorRef, dataset: DatasetRef): Unit = {
    subscriptions = subscriptions.subscribe(subscriber, dataset)
    subscriptions.watchers foreach (sendSubscriptions(_))
    context watch subscriber
  }

  /**
    * Unsubscribes a subscriber from all dataset shard updates.
    * Sends watchers the updated subscriptions.
    * INTERNAL API. Idempotent.
    *
    * @param subscriber the cluster member removed from the cluster
    *                or regular subscriber unsubscribing
    */
  private def unsubscribe(subscriber: ActorRef): Unit = {
    subscriptions = subscriptions unsubscribe subscriber
    subscriptions.watchers foreach (sendSubscriptions(_))
    context unwatch subscriber
  }

  /** Sends subscribers for the dataset to the requester. If the subscription
    * does not exist the subscribers will be empty.
    *
    * INTERNAL API. Read-only.
    */
  private def sendSubscribers(ds: DatasetRef, origin: ActorRef): Unit =
    origin ! Subscribers(subscriptions.subscribers(ds), ds)

  /** Sends subscriptions to requester.
    *
    * INTERNAL API. Read-only.
    */
  private def sendSubscriptions(origin: ActorRef): Unit =
    origin ! subscriptions

  /** Removes the dataset from all subscriptions and data structures
    *
    * INTERNAL API. Idempotent.
    *
    * @param dataset the dataset to remove if it was setup
    */
  private def removeDataset(dataset: DatasetRef): Unit = {
    subscriptions = subscriptions - dataset
    shardMappers remove dataset
    resources remove dataset
    metrics(dataset).shutdown()
    metrics remove dataset
  }

  /** Resets all state.
    * INTERNAL API.
    */
  private def reset(origin: ActorRef): Unit = {
    shardMappers.clear()
    resources.clear()
    metrics.values.foreach(_.shutdown())
    metrics.clear()
    subscriptions = subscriptions.clear
    origin ! NodeProtocol.StateReset
  }
}

object ShardSubscriptions {
  import ShardAssignmentStrategy.DatasetShards

  val Empty = ShardSubscriptions(Set.empty, Set.empty)

  final case class Subscribers(subscribers: Set[ActorRef], dataset: DatasetRef)

  sealed trait SubscriptionProtocol
  sealed trait ShardAssignmentProtocol

  /** Command to add a subscription. */
  private[coordinator] final case class AddDataset(setup: SetupDataset,
                                                   dataset: Dataset,
                                                   coordinators: Set[ActorRef],
                                                   ackTo: ActorRef
                                                  ) extends ShardAssignmentProtocol

  /** Ack by ShardStatusActor to it's parent, [[filodb.coordinator.NodeClusterActor]],
    * upon it sending `AddMember`. Command to start ingestion for dataset.
    */
  private[coordinator] final case class DatasetAdded(dataset: Dataset,
                                                     source: IngestionSource,
                                                     shards: Map[ActorRef, Seq[Int]],
                                                     ackTo: ActorRef
                                                    ) extends ShardAssignmentProtocol

  sealed trait ShardCoordCommand extends SubscriptionProtocol

  /** Usable by FiloDB clients.
    * Internally used by Coordinators to subscribe a new Query actor to it's dataset.
    *
    * @param subscriber the actor subscribing to the `ShardMapper` status updates
    * @param dataset    the `DatasetRef` key for the `ShardMapper`
    */
  final case class Subscribe(subscriber: ActorRef, dataset: DatasetRef) extends ShardCoordCommand

  private[coordinator] case object SubscribeAll extends ShardCoordCommand

  /** Used only by the cluster actor to add/remove coordinators to all datasets and update shard assignments
    * INTERNAL API.
    */
  private[coordinator] final case class AddMember(coordinator: ActorRef, addr: Address) extends ShardCoordCommand
  private[coordinator] final case class RemoveMember(coordinator: ActorRef) extends ShardCoordCommand

  /** Ack returned by shard actor to cluster actor on successful coordinator subscribe. */
  private[coordinator] final case class CoordinatorAdded(
    coordinator: ActorRef, shards: Seq[DatasetShards], addr: Address) extends SubscriptionProtocol

  /** Ack returned by shard actor to cluster actor on successful coordinator remove. */
  private[coordinator] final case class CoordinatorRemoved(
    coordinator: ActorRef, shards: Seq[DatasetShards]) extends SubscriptionProtocol

  /** Unsubscribes a subscriber. */
  final case class Unsubscribe(subscriber: ActorRef) extends SubscriptionProtocol

  private[coordinator] final case class GetSubscribers(dataset: DatasetRef) extends SubscriptionProtocol

  private[coordinator] final case class RemoveDataset(dataset: DatasetRef) extends SubscriptionProtocol

  private[coordinator] case object GetSubscriptions extends SubscriptionProtocol

  /** Returns a mapper snapshot for a specific dataset if it exists. */
  private[coordinator] final case class GetSnapshot(dataset: DatasetRef) extends SubscriptionProtocol

  private[coordinator] final case class RecoverShardState(ref: DatasetRef, map: ShardMapper)
  private[coordinator] final case class RecoverSubscriptions(subscriptions: ShardSubscriptions)

  private[coordinator] case object Reset extends SubscriptionProtocol
  private[coordinator] case object ResetComplete extends SubscriptionProtocol

}

private[coordinator] final case class ShardSubscriptions(subscriptions: Set[ShardSubscription],
                                                         watchers: Set[ActorRef]) {

  def subscribe(watcher: ActorRef): ShardSubscriptions =
    copy(subscriptions = subscriptions.map(_ + watcher), watchers = watchers + watcher)

  def subscribe(subscriber: ActorRef, to: DatasetRef): ShardSubscriptions =
    subscription(to).map { ss =>
      copy(subscriptions = (subscriptions - ss) + (ss + subscriber))
    }.getOrElse(this)

  def unsubscribe(subscriber: ActorRef): ShardSubscriptions =
    copy(subscriptions = subscriptions.map(_ - subscriber), watchers = watchers - subscriber)

  def subscription(dataset: DatasetRef): Option[ShardSubscription] =
    subscriptions.collectFirst { case s if s.dataset == dataset => s }

  def subscribers(dataset: DatasetRef): Set[ActorRef] =
    subscription(dataset).map(_.subscribers).getOrElse(Set.empty)

  //scalastyle:off method.name
  def :+(s: ShardSubscription): ShardSubscriptions =
    subscription(s.dataset).map(x => this)
      .getOrElse(copy(subscriptions = this.subscriptions + s))

  def -(dataset: DatasetRef): ShardSubscriptions =
    subscription(dataset).map { s =>
      copy(subscriptions = this.subscriptions - s)}
      .getOrElse(this)

  def clear: ShardSubscriptions =
    this copy (subscriptions = Set.empty, watchers = Set.empty)

}

private[coordinator] final case class ShardSubscription(dataset: DatasetRef, subscribers: Set[ActorRef]) {

  def +(subscriber: ActorRef): ShardSubscription =
    copy(subscribers = this.subscribers + subscriber)

  def -(subscriber: ActorRef): ShardSubscription =
    copy(subscribers = this.subscribers - subscriber)

}