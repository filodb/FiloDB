package filodb.core

import com.typesafe.scalalogging.StrictLogging
import org.scalactic.Explicitly
import org.scalatest._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

trait AbstractSpec extends WordSpec with Matchers
  with BeforeAndAfterAll with BeforeAndAfterEach
  with Eventually with Explicitly with IntegrationPatience
  with StrictLogging
