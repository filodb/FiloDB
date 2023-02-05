package filodb.query

import io.circe.parser
import io.circe.generic.auto._
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

  it("should parse LabelSampl") {
    val inputString = """{
                         |    "status": "success",
                         |    "data": ["data1","data2","data3"]
                         |}""".stripMargin

    parser.decode[MetadataSuccessResponse](inputString) match {
      case Right(labels) => labels shouldEqual
        MetadataSuccessResponse(List(LabelSampl("data1"), LabelSampl("data2"), LabelSampl("data3")), "success", None, None)
      case Left(ex) => throw ex
    }
  }

  it("should parse MetadataMapSampl") {
    val expected = Seq(
      MetadataMapSampl(Map("tag1" -> "value1", "tag2" -> "value2", "tag3" -> "value3")),
      MetadataMapSampl(Map("tag11" -> "value11", "tag22" -> "value22", "tag33" -> "value33"))
    )
    val inputString =
      """{
        |    "status": "success",
        |    "data": [
        |       {
        |		      "tag1": "value1",
        |		      "tag2": "value2",
        |		      "tag3": "value3"
        |       },
        |       {
        |		      "tag11": "value11",
        |		      "tag22": "value22",
        |		      "tag33": "value33"
        |       }
        |     ]
        |}""".stripMargin

    parser.decode[MetadataSuccessResponse](inputString) match {
      case Right(response) => response shouldEqual MetadataSuccessResponse(expected)
      case Left(ex) => throw ex
    }
  }

  /**
   *   final case class LabelCardinalitySampl(metric: Map[String, String],
                                       cardinality: Seq[Map[String, String]]) extends MetadataSampl
   */

  it("should parse LabelCardinalitySampl") {
    val expected = Seq(
      LabelCardinalitySampl(
        Map("_ws_" -> "demo", "_ns_" -> "App-0", "_metric_" -> "heap_usage"),
        Seq(
          Map("tag" -> "instance", "count" -> "10"),
          Map("tag" -> "host", "count" -> "5"),
          Map("tag" -> "datacenter", "count" -> "2")
        )
      ),
      LabelCardinalitySampl(Map("_ws_" -> "demo", "_ns_" -> "App-1", "_metric_" -> "request_latency"),
        Seq(
          Map("tag" -> "instance", "count" -> "6"),
          Map("tag" -> "host", "count" -> "2"),
          Map("tag" -> "datacenter", "count" -> "2")
        ))
    )
    val inputString =
      """{
        |    "status": "success",
        |    "data": [
        |       {
        |		      "metric": {
        |           "_ws_": "demo",
        |		        "_ns_": "App-0",
        |		        "_metric_": "heap_usage"
        |          },
        |          "cardinality":
        |             [
        |               {
        |                 "tag": "instance",
        |                 "count": "10"
        |               },
        |               {
        |                 "tag": "host",
        |                 "count": "5"
        |               },
        |               {
        |                 "tag": "datacenter",
        |                 "count": "2"
        |               }
        |             ]
        |       },
        |       {
        |		      "metric": {
        |           "_ws_": "demo",
        |		        "_ns_": "App-1",
        |		        "_metric_": "request_latency"
        |          },
        |          "cardinality":
        |             [
        |               {
        |                 "tag": "instance",
        |                 "count": "6"
        |               },
        |               {
        |                 "tag": "host",
        |                 "count": "2"
        |               },
        |               {
        |                 "tag": "datacenter",
        |                 "count": "2"
        |               }
        |             ]
        |       }
        |     ]
        |}""".stripMargin

    parser.decode[MetadataSuccessResponse](inputString) match {
      case Right(response) => response shouldEqual MetadataSuccessResponse(expected)
      case Left(ex) => throw ex
    }
  }

  it("should parse TsCardinalitiesSampl") {
    val expected = Seq(
      TsCardinalitiesSampl(
        Map("_ws_" -> "demo", "_ns_" -> "App-0", "_metric_" -> "heap_usage"),
        Map("active" -> 2, "total" -> 3)),
      TsCardinalitiesSampl(
        Map("_ws_" -> "demo", "_ns_" -> "App-1"),
        Map("active" -> 6, "total" -> 8)),
      TsCardinalitiesSampl(
        Map("_ws_" -> "demo"),
        Map("active" -> 7, "total" -> 10))
    )
    val inputString =
      """{
        |  "status": "success",
        |  "data": [
        |    {
        |	     "group": {
        |         "_ws_": "demo",
        |	       "_ns_": "App-0",
        |	       "_metric_": "heap_usage"
        |      },
        |      "cardinality": {
        |        "active": 2,
        |        "total": 3
        |      }
        |    },
        |    {
        |	     "group": {
        |        "_ws_": "demo",
        |	       "_ns_": "App-1"
        |      },
        |      "cardinality": {
        |        "active": 6,
        |        "total": 8
        |      }
        |    },
        |    {
        |      "group": {
        |        "_ws_": "demo"
        |      },
        |      "cardinality": {
        |        "active": 7,
        |        "total": 10
        |      }
        |    }
        |  ]
        |}""".stripMargin

    parser.decode[MetadataSuccessResponse](inputString) match {
      case Right(response) => response shouldEqual MetadataSuccessResponse(expected)
      case Left(ex) => throw ex
    }
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
      case Left(ex) => throw ex
    }
  }

  it("should parse sttdev aggregateResponse") {
    val input = """[{
                  |  "status": "success",
                  |  "data": {
                  |    "resultType": "matrix",
                  |    "result": [
                  |      {
                  |        "metric": {
                  |
                  |        },
                  |        "aggregateResponse": {
                  |          "aggregateValues": [
                  |            [
                  |              1603920650,
                  |              "NaN",
                  |              "NaN",
                  |              0
                  |            ],
                  |            [
                  |              1603920740,
                  |              "0.0",
                  |              "16.068496952984738",
                  |              1
                  |            ]
                  |          ],
                  |          "function": "stdval"
                  |        }
                  |      }
                  |    ]
                  |  },
                  |  "errorType": null,
                  |  "error": null
                  |}]""".stripMargin
    val expectedResult =List(StdValSampl(1603920650,Double.NaN, Double.NaN, 0),
      StdValSampl(1603920740,0,16.068496952984738,1)
      )

    parser.decode[List[SuccessResponse]](input) match {
      case Right(successResponse) => val aggregateResponse = successResponse.head.data.result.head.aggregateResponse.get
        aggregateResponse.function shouldEqual("stdval")
        aggregateResponse.aggregateSampl.map(_.asInstanceOf[StdValSampl]).zip(expectedResult).foreach {
          case (res, ex) => if (res.mean.isNaN) {
            ex.mean.isNaN shouldEqual(true)
            ex.stddev.isNaN shouldEqual true
            ex.count shouldEqual(res.count)
            ex.timestamp shouldEqual(res.timestamp)
          } else ex shouldEqual(res)
        }
      case Left(ex) => throw ex
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

  it("should parse remote error response without queryStats") {
    val input = """[{
                  |  "status" : "error",
                  |  "data" : null,
                  |  "errorType" : "query_materialization_failed",
                  |  "error" : "Shard: 2 is not available"
                  |}]""".stripMargin

    parser.decode[List[ErrorResponse]](input) match {
      case Right(errorResponse) => errorResponse.head shouldEqual(ErrorResponse("query_materialization_failed",
        "Shard: 2 is not available", "error", None))
      case Left(ex)             => throw ex
    }
  }

  it("should parse remote error response with queryStats") {
    val input = """[{
                  |  "status" : "error",
                  |  "data" : null,
                  |  "errorType" : "query_materialization_failed",
                  |  "error" : "Shard: 2 is not available",
                  |  "queryStats": [
                  |        {
                  |            "group": [
                  |                "local",
                  |                "raw",
                  |                "ws1",
                  |                "ns1",
                  |                "metric1"
                  |            ],
                  |            "timeSeriesScanned": 24,
                  |            "dataBytesScanned": 38784,
                  |            "resultBytes": 15492,
                  |            "cpuNanos"": 434999
                  |        }
                  |    ]
                  |}]""".stripMargin
    val qs = QueryStatistics(Seq("local", "raw", "ws1", "ns1", "metric1"), 24, 38784, 15492, 434999)
    parser.decode[List[ErrorResponse]](input) match {
      case Right(errorResponse) =>
        errorResponse.head.errorType shouldEqual "query_materialization_failed"
        errorResponse.head.error shouldEqual "Shard: 2 is not available"
        errorResponse.head.status shouldEqual "error"
        errorResponse.head.queryStats.isDefined shouldEqual true
        errorResponse.head.queryStats.get.size shouldEqual 1
        errorResponse.head.queryStats.get.head shouldBe qs
      case Left(ex)             => throw ex
    }
  }

  it("should parse label cardinality response correctly") {
    val input =
      """[
        |{
        |  "status": "success",
        |  "data": [
        |    {
        |      "cardinality": [
        |        {
        |          "count": "1",
        |          "label": "app"
        |        },
        |        {
        |          "count": "1",
        |          "label": "_type_"
        |        },
        |        {
        |          "count": "1",
        |          "label": "__name__"
        |        },
        |        {
        |          "count": "1",
        |          "label": "_ns_"
        |        },
        |        {
        |          "count": "1",
        |          "label": "_ws_"
        |        },
        |        {
        |          "count": "1",
        |          "label": "host"
        |        },
        |        {
        |          "count": "1",
        |          "label": "version"
        |        }
        |      ],
        |      "metric": {
        |        "_ns_": "App-0",
        |        "_metric_": "go_info",
        |        "_ws_": "filodb-demo"
        |      }
        |    }
        |  ],
        |  "errorType": null,
        |  "error": null
        |}
        |]""".stripMargin

    val lc = Seq(LabelCardinalitySampl(
      Map("_ws_"      -> "filodb-demo",
          "_ns_"      -> "App-0",
          "_metric_"  -> "go_info"),
      List(
        Map("label" -> "app", "count" -> "1"),
        Map("label" -> "_type_", "count" -> "1"),
        Map("label" -> "__name__", "count" -> "1"),
        Map("label" -> "_ns_", "count" -> "1"),
        Map("label" -> "_ws_", "count" -> "1"),
        Map("label" -> "host", "count" -> "1"),
        Map("label" -> "version", "count" -> "1"),
    )))
    val expected = MetadataSuccessResponse(lc)
    val resp: Either[io.circe.Error, List[MetadataSuccessResponse]] =
      parser.decode[List[MetadataSuccessResponse]](input)
    resp match {
      case Right(success :: Nil)        =>
            success shouldEqual expected

      case Left(ex)                     => throw ex
      case _                            => fail("Expected to see a Right with just one element")
    }
  }

  it("should parse remote partial response") {
    val input = """[{
                  |  "status" : "partial",
                  |  "data" : {
                  |        "result": [
                  |            {
                  |                "metric": {
                  |                    "__name__": "my_counter",
                  |                    "_ns_": "test_001",
                  |                    "_partIds_": "2530",
                  |                    "_shards_": "25",
                  |                    "_step_": "10",
                  |                    "_type_": "prom-counter",
                  |                    "_ws_": "demo",
                  |                    "instance": "c70cac88-928e-4905-9f37-c1c6ed27cf27"
                  |                },
                  |                "value": [
                  |                    1619636156,
                  |                    "1.8329092E7"
                  |                ]
                  |            }
                  |        ],
                  |        "resultType": "vector"
                  |    },
                  |  "partial": true,
                  |  "message": "Result may be partial since some shards are still bootstrapping",
                  |  "errorType": null,
                  |  "error": null
                  |}]""".stripMargin

    parser.decode[List[SuccessResponse]](input) match {
      case Right(successResponse) => {
        val response = successResponse.head
        response.partial.get shouldEqual true
        response.message.get shouldEqual "Result may be partial since some shards are still bootstrapping"
        response.status shouldEqual "partial"
        val result = response.data.result.head
        result.metric.size shouldEqual 8
        result.value.size shouldEqual 1
      }
      case Left(ex) => throw ex
    }
  }
}
