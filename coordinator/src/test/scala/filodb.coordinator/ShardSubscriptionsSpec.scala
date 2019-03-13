package filodb.coordinator

import akka.testkit.TestProbe

import filodb.core.DatasetRef

class ShardSubscriptionsSpec extends AkkaSpec {

  private val extension = FilodbCluster(system)

  private val dataset1 = DatasetRef("one")
  private val dataset2 = DatasetRef("two")

  private val subscribers = Set(TestProbe().ref, TestProbe().ref)

  "ShardSubscription" must {
    "add unseen subscribers, not add seen subscribers" in {
      var subscription = ShardSubscription(dataset1, Set.empty)
      subscription = subscription + self
      subscription.subscribers.size shouldEqual 1
      subscription.subscribers.head shouldEqual self
      subscription = subscription + TestProbe().ref
      subscription.subscribers.size shouldEqual 2
      subscription = subscription + self
      subscription.subscribers.size shouldEqual 2
    }
    "remove seen subscribers, not remove unseen subscribers" in {
      var subscription = ShardSubscription(dataset1, Set.empty)
      subscription = subscription + self
      subscription = subscription + TestProbe().ref
      subscription.subscribers.size shouldEqual 2
      subscription = subscription - TestProbe().ref
      subscription.subscribers.size shouldEqual 2
    }
  }
  "ShardSubscriptions" must {
    "add only new subscriptions" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions.subscriptions.isEmpty should be (true)

      subscriptions :+= ShardSubscription(dataset1, Set.empty)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions :+= ShardSubscription(dataset1, Set(subscribers.head))
      subscriptions.subscriptions.size shouldEqual 1 // seen
    }
    "subscribe only unseen subscribers for seen datasets that do not already have those subscribers" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions :+= ShardSubscription(dataset1, subscribers)
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size

      subscriptions = subscriptions.subscribe(self, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size + 1
      subscriptions = subscriptions.subscribe(self, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size + 1

      subscriptions = subscriptions.subscribe(TestProbe().ref, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size + 2
    }
    "unsubscribe a subscriber" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions :+= ShardSubscription(dataset1, subscribers)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size

      subscriptions = subscriptions.unsubscribe(subscribers.head)
      subscriptions.subscriptions.forall(s => !s.subscribers.contains(subscribers.head))
      subscriptions.subscribers(dataset1).size shouldEqual subscribers.size - 1
    }
    "subscribe to all (internal)" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions :+= ShardSubscription(dataset1, subscribers)
      subscriptions :+= ShardSubscription(dataset2, subscribers)
      subscriptions.subscriptions.forall(_.subscribers.size == 2) shouldEqual true
      val probe = TestProbe()
      subscriptions = subscriptions.subscribe(probe.ref)
      subscriptions.subscriptions.forall(_.subscribers.size == 3) shouldEqual true
      subscriptions.watchers shouldEqual Set(probe.ref)
      subscriptions = subscriptions.unsubscribe(probe.ref)
      subscriptions.watchers.isEmpty shouldEqual true
    }
    "remove a subscription" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions :+= ShardSubscription(dataset1, Set.empty)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions = subscriptions - dataset1
      subscriptions.subscriptions.isEmpty should be (true)
    }
    "reset state" in {
      var subscriptions = ShardSubscriptions.Empty
      subscriptions :+= ShardSubscription(dataset1, subscribers)
      subscriptions = subscriptions.clear
      subscriptions.subscriptions.isEmpty should be (true)
    }
  }
}
