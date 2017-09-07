package filodb.coordinator

import akka.actor._

import filodb.core.DatasetRef

class Parent extends NamingAwareBaseActor {

  import ActorName.{Ingestion, Query}

  override def receive: Actor.Receive = {
    case Parent.Create(dataset) =>
      context.actorOf(Props[Child], s"$Ingestion-$dataset")
      context.actorOf(Props[Child], s"$Query-$dataset")
    case Parent.GetAllCreated(prefix) =>
      sender() ! Parent.Created(childrenForType(prefix))
    case Parent.GetCreated(prefix, ds) =>
      sender() ! Parent.Created(childFor(ds, prefix).toSet)
    case Parent.GetDsCreated(ds) =>
      val pair = context.children filter (_.path.name endsWith ds.dataset)
      sender() ! Parent.Created(pair)
    case other: ActorRef =>
      sender() ! isSame(other, self)
  }
}
class Child extends Actor {
  override def receive: Actor.Receive = { case e => }
}

object Parent {
  final case class Create(dataset: DatasetRef)
  final case class Created(children: Iterable[ActorRef])
  final case class GetAllCreated(prefix: String)
  final case class GetCreated(prefix: String, ds: DatasetRef)
  final case class GetDsCreated(ds: DatasetRef)
}

class NamingAwareBaseActorSpec extends AkkaSpec {

  import ActorName.{CoordinatorName, Ingestion, Query}

  "NamingAwareBaseActor" must {
    val datasets = Set(DatasetRef("one"), DatasetRef("two"), DatasetRef("three"))
    val arr = datasets.toArray

    val parent = system.actorOf(Props[Parent], CoordinatorName)
    datasets foreach (parent ! Parent.Create(_))

    "get the unique child on the node for the specified dataset and type by naming convention" in {
      datasets foreach { ds =>

        Set(Ingestion, Query) foreach { prefix =>
          parent ! Parent.GetCreated(prefix, ds)
          expectMsgPF() {
            case Parent.Created(children) =>
              children.size shouldEqual 1
              children.forall(a => a.path.name == s"$prefix-${ds.dataset}") should be(true)
          }
        }
      }
    }
    "get the two children of any type (Ingestion|Query) for a given dataset by naming convention" in {

      datasets foreach { ds =>
        parent ! Parent.GetDsCreated(ds)
        expectMsgPF() {
          case Parent.Created(dsPair) =>
            dsPair.size shouldEqual 2 // one MemstoreCoordActor, one QueryActor
            dsPair.forall(a => a.path.name endsWith ds.dataset)
        }
      }
    }
    "get all children of a specific type for all datasets by naming convension and confirm naming scheme" in {

      nameIsPrefixAndDataset(Ingestion)
      nameIsPrefixAndDataset(Query)

      def nameIsPrefixAndDataset(prefix: String): Unit = {
        parent ! Parent.GetAllCreated(prefix)
        expectMsgPF() {
          case Parent.Created(children) =>
            children.size shouldEqual datasets.size
            children.forall(a => a.path.name startsWith prefix)
            val names = children.map { name =>
              val lr = name.path.name.split("-")
              lr(1) -> lr(0)
            }.toMap

            for {
              ds   <- datasets
              name = names(ds.dataset)
            } {
              name startsWith prefix should be(true)
              name endsWith prefix should be(true)
            }
        }
      }
    }
    "determine if 'other' actor is same as self or if two actors are the same" in {
      parent ! self
      expectMsgPF() { case isSame => isSame shouldEqual false }

      parent ! parent
      expectMsgPF() { case isSelf => isSelf shouldEqual true }
    }
  }
}
