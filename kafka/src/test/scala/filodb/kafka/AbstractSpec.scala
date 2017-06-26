package filodb.kafka

import org.scalactic.Explicitly
import org.scalatest._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

trait BaseSpec extends Suite with MustMatchers
  with BeforeAndAfterAll with BeforeAndAfterEach
  with Eventually with IntegrationPatience with Explicitly

trait AbstractSpec extends WordSpec with BaseSpec

trait AbstractSuite extends FeatureSpec with BaseSpec with GivenWhenThen
