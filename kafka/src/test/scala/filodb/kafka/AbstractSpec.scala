package filodb.kafka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.scalalogging.StrictLogging
import org.scalactic.Explicitly
import org.scalatest._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

trait BaseSpec extends Suite with MustMatchers
  with BeforeAndAfterAll with BeforeAndAfterEach
  with Eventually with IntegrationPatience with Explicitly

trait AbstractSpec extends WordSpec with BaseSpec

trait AbstractSuite extends FeatureSpec with BaseSpec with GivenWhenThen

abstract class AbstractAkkaSpec(name: String) extends TestKit(ActorSystem(name))
 with BaseSpec with WordSpecLike with ImplicitSender with StrictLogging

trait ConfigSpec extends AbstractSpec {
  val FullTestPropsPath = "./src/test/resources/full-test.properties"
}