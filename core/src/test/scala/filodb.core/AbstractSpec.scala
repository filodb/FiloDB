package filodb.core

import com.typesafe.scalalogging.StrictLogging
import org.scalactic.Explicitly
import org.scalatest._
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}

trait AbstractSpec extends WordSpec with Matchers
  with BeforeAndAfterAll with BeforeAndAfterEach
  with Eventually with Explicitly with IntegrationPatience
  with StrictLogging


trait AsyncTest extends Suite with Matchers with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience =   // TODO: or just use IntegrationPatience, if that's enough
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))
}
