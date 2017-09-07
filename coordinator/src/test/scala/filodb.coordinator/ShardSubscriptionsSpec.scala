package filodb.coordinator

import akka.testkit.TestProbe

import filodb.core.DatasetRef

class ShardSubscriptionsSpec extends AkkaSpec {

  private val extension = FilodbCluster(system)

  private val dataset1 = DatasetRef("one")
  private val dataset2 = DatasetRef("two")

  private val coordinators = Set(TestProbe().ref, TestProbe().ref)

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
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions.subscriptions.isEmpty should be (true)

      subscriptions :+= ShardSubscription(dataset1, Set.empty)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions :+= ShardSubscription(dataset1, Set(coordinators.head))
      subscriptions.subscriptions.size shouldEqual 1 // seen
    }
    "subscribe only unseen subscribers for seen datasets that do not already have those subscribers" in {
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions :+= ShardSubscription(dataset1, coordinators)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size

      subscriptions = subscriptions.subscribe(self, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size + 1
      subscriptions = subscriptions.subscribe(self, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size + 1

      subscriptions = subscriptions.subscribe(TestProbe().ref, dataset1)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size + 2
    }
    "unsubscribe a subscriber" in {
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions :+= ShardSubscription(dataset1, coordinators)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size

      subscriptions = subscriptions.unsubscribe(coordinators.head)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size - 1
    }
    "subscribe all provided coordinators on dataset setup" in {
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions :+= ShardSubscription(dataset1, coordinators)
      subscriptions.subscribers(dataset1).size shouldEqual coordinators.size
    }
    "remove a subscription" in {
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions :+= ShardSubscription(dataset1, Set.empty)
      subscriptions.subscriptions.size shouldEqual 1
      subscriptions = subscriptions - dataset1
      subscriptions.subscriptions.isEmpty should be (true)
    }
    "reset state" in {
      var subscriptions = ShardSubscriptions(Set.empty)
      subscriptions :+= ShardSubscription(dataset1, coordinators)
      subscriptions = subscriptions.clear
      subscriptions.subscriptions.isEmpty should be (true)
    }
  }
}
