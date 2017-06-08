package filodb.ingestion

import akka.actor.Extension
import akka.actor.ActorSystem
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.ExtendedActorSystem

private[filodb] object FiloDBIngestion extends ExtensionId[FiloDBIngestion] with ExtensionIdProvider {

  //The lookup method is required by ExtensionIdProvider,
  // so we return ourselves here, this allows us
  // to configure our extension to be loaded when
  // the ActorSystem starts up
  override def lookup: ExtensionId[_ <: Extension] = FiloDBIngestion

  //This method will be called by Akka
  // to instantiate our Extension
  override def createExtension(system: ExtendedActorSystem) = new FiloDBIngestion(system)

  /** Java API: retrieve the Count extension for the given system. */
  override def get(system: ActorSystem): FiloDBIngestion = super.get(system)

}

private[filodb] class FiloDBIngestion(system: ExtendedActorSystem) extends Extension {

  //This is the operation this Extension provides
  // def increment() = counter.incrementAndGet()
}
