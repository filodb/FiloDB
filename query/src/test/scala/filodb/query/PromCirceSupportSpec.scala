package filodb.query

import io.circe.parser
import io.circe.generic.auto._
//import io.circe.syntax.EncoderOps
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PromCirceSupportSpec extends AnyFunSpec with Matchers with ScalaFutures {

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._

  it("should parse Sampl") {
    val inputString =
      """
        |[
        |	[
        |		1600102672,
        |		"1.2"
        |	],
        |	[
        |		1600102687,
        |		"3.1"
        |	]
        |]
        |""".stripMargin

   parseAndValidate(inputString, List(Sampl(1600102672,1.2), Sampl(1600102687,3.1)))
  }

  it("should parse aggregateResponse") {
    val input = """[{
                  |	"status": "success",
                  |	"data": {
                  |		"resultType": "matrix",
                  |		"result": [{
                  |			"metric": {
                  |
                  |			},
                  |			"aggregateResponse": {
                  |				"aggregateValues": [
                  |					[
                  |						1601491649,
                  |						"15.186417982460787",
                  |						5
                  |					],
                  |					[
                  |						1601491679,
                  |						"14.891293858511071",
                  |						6
                  |					],
                  |					[
                  |						1601491709,
                  |						"14.843819532173134",
                  |						7
                  |					],
                  |         [
                  |						1601491719,
                  |						"NaN",
                  |						7
                  |					]
                  |
                  |				],
                  |				"function": "avg"
                  |			}
                  |		}]
                  |	},
                  |	"errorType": null,
                  |	"error": null
                  |}]""".stripMargin
    val expectedResult =List(AvgSampl(1601491649,15.186417982460787,5),
      AvgSampl(1601491679,14.891293858511071,6), AvgSampl(1601491709,14.843819532173134,7), AvgSampl(1601491719,
        Double.NaN, 7))

    parser.decode[List[SuccessResponse]](input) match {
      case Right(successResponse) => val aggregateResponse = successResponse.head.data.result.head.aggregateResponse.get
        aggregateResponse.function shouldEqual("avg")
        aggregateResponse.aggregateSampl.map(_.asInstanceOf[AvgSampl]).zip(expectedResult).foreach {
          case (res, ex) => if (res.value.isNaN) {
            ex.value.isNaN shouldEqual(true)
            ex.count shouldEqual(res.count)
            ex.timestamp shouldEqual(ex.timestamp)
          } else ex shouldEqual(res)
        }
      case Left(ex) => println(ex)
    }
  }

   def parseAndValidate(input: String, expectedResult: List[DataSampl]): Unit = {
     parser.decode[List[DataSampl]](input) match {
       case Right(samples) =>
         samples.zip(expectedResult).foreach {
           case (val1: Sampl, val2: Sampl) => {
                 val1.timestamp shouldEqual(val2.timestamp)
                 if (val1.value.isNaN) val2.value.isNaN shouldEqual true
                 else val1.value shouldEqual val2.value
               }

           case (val1: HistSampl, val2: HistSampl) => {
             val1.timestamp shouldEqual(val2.timestamp)
             val1.buckets shouldEqual val2.buckets
           }

           case _ => samples.sameElements(expectedResult)
         }
       case Left(ex) => throw ex
     }
  }
}
