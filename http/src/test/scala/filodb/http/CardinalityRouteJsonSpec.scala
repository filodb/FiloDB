package filodb.http

import io.circe.Printer
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.query.{MetadataSampl, TsCardinalitiesSamplV2}
import filodb.query.PromCirceSupport.decodeMetadataSampl

class CardinalityRouteJsonSpec extends AnyFunSpec with Matchers {

  private val printer = Printer.spaces2.copy(dropNullValues = true)

  private val sample = TsCardinalitiesSamplV2(
    Map("_ws_" -> "demo", "_ns_" -> "App-0"),
    Map("active" -> 4L, "billable" -> 4L, "shortTerm" -> 6L, "longTerm" -> 0L),
    "raw", "prometheus")

  it("should render the flat wire shape the query-service decoder expects") {
    val json = printer.print(TsCardinalitiesResponse(Seq(sample)).asJson)
    // scalastyle:off
    println(json)
    // scalastyle:on
    // the discriminator wrapper that sealed-trait auto derivation would produce must NOT appear
    json should not include "TsCardinalitiesSamplV2"
    // and PromCirceSupport must be able to read each element back
    val elem = io.circe.parser.parse(json).toOption.get
      .hcursor.downField("data").downArray.focus.get
    decode[MetadataSampl](elem.noSpaces) shouldEqual Right(sample)
  }

  it("should omit partial/message on a complete result and include them on a partial one") {
    printer.print(TsCardinalitiesResponse(Seq(sample)).asJson) should not include "partial"

    val partial = TsCardinalitiesResponse(Seq(sample),
      partial = Some(true), message = Some("shards not counted: 2,3"))
    val json = printer.print(partial.asJson)
    // scalastyle:off
    println(json)
    // scalastyle:on
    json should include ("\"partial\" : true")
    json should include ("shards not counted: 2,3")
  }
}
