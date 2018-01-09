package filodb.coordinator

import akka.actor._

import filodb.core.DatasetRef

object ShardSubscriptions {

  val Empty = ShardSubscriptions(Set.empty, Set.empty)

  private[coordinator] case object SubscribeAll

  /** Unsubscribes a subscriber. */
  final case class Unsubscribe(subscriber: ActorRef)
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