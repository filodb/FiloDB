package filodb.query

import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import io.circe.syntax._

import filodb.query.AggregationOperator.Avg

object PromCirceSupport {
  // necessary to encode sample in promql response as an array with long and double value as string
  // Specific encoders for *Sampl types
  implicit val encodeSampl: Encoder[DataSampl] = Encoder.instance {
    case s @ Sampl(t, v)                                => Json.fromValues(Seq(t.asJson, v.toString.asJson))
    case h @ HistSampl(t, b)                            => Json.fromValues(Seq(t.asJson, b.asJson))
  }

  implicit val encodeMetadataSampl: Encoder[MetadataSampl] = Encoder.instance {
    case m @ MetadataMapSampl(v) => Json.fromValues(Seq(v.asJson))
    case l @ LabelSampl(v) => v.asJson
    // Where are these used? added to make compiler happy
    case l @ LabelCardinalitySampl(group, cardinality)  =>
      Json.fromValues(Seq(group.asJson, cardinality.asJson))
    case t @ TsCardinalitiesSampl(group, cardinality) =>
      Json.fromValues(Seq(group.asJson, cardinality.asJson))
    case a @ TsCardinalitiesSamplV2(group, cardinality, dataset, _type) =>
        Json.fromValues(Seq(group.asJson, cardinality.asJson, dataset.asJson, _type.asJson))
  }

  implicit val decodeAvgSample: Decoder[AvgSampl] = new Decoder[AvgSampl] {
    final def apply(c: HCursor): Decoder.Result[AvgSampl] = {
      for { timestamp <- c.downArray.as[Long]
            value <- c.downArray.right.as[String]
            count <- c.downArray.right.right.as[Long]
            } yield {
        AvgSampl(timestamp, value.toDouble, count)
      }
    }
  }

    implicit val decodeStdValSampl: Decoder[StdValSampl] = new Decoder[StdValSampl] {
      final def apply(c: HCursor): Decoder.Result[StdValSampl] = {
        for {timestamp <- c.downArray.as[Long]
             stddev <- c.downArray.right.as[String]
             mean <- c.downArray.right.right.as[String]
             count <- c.downArray.right.right.right.as[Long]
             } yield {
          StdValSampl(timestamp, stddev.toDouble, mean.toDouble, count)
        }
      }
    }

  implicit val decodeMetadataSampl: Decoder[MetadataSampl] = new Decoder[MetadataSampl] {
    def apply(c: HCursor): Decoder.Result[MetadataSampl] = {
      // TODO: Not the best way to find if this is a cardinality response, is there a better way?
      if (c.downField("cardinality").focus.nonEmpty) {
        // LabelCardinality has "metric" map; TsCardinalities has "group" map
        if (c.downField("metric").focus.nonEmpty) {
          for {
            metric <- c.get[Map[String, String]]("metric")
            card <- c.get[Seq[Map[String, String]]]("cardinality")
          } yield LabelCardinalitySampl(metric, card)
        } else if (c.downField("group").focus.nonEmpty) {
            // V2 Cardinality API also has a cluster field
            if (c.downField("dataset").focus.nonEmpty) {
              for {
                group <- c.get[Map[String, String]]("group")
                card <- c.get[Map[String, Int]]("cardinality")
                dataset <- c.get[String]("dataset")
                _type <- c.get[String]("_type")
              } yield TsCardinalitiesSamplV2(group, card, dataset, _type)
            }
            else {
              for {
                group <- c.get[Map[String, String]]("group")
                card <- c.get[Map[String, Int]]("cardinality")
              } yield TsCardinalitiesSampl(group, card)
            }
        } else {
          throw new IllegalArgumentException("could not decode any expected cardinality-related field")
        }
      } else if (c.value.isString) {
        for { label <- c.as[String] } yield LabelSampl(label)
      } else {
        for { metadataMap <- c.as[Map[String, String]] } yield MetadataMapSampl(metadataMap)
      }
    }
  }

  implicit val doubleMapDecoder: Decoder[Map[String, Double]] = new Decoder[Map[String, Double]]{
    final def apply(c: HCursor): Decoder.Result[Map[String, Double]]= {
      c.focus match {
        case Some(map) if map.isObject  =>
          Right(map.asObject.get.toMap.mapValues(x => {
                if (x.isString)
                  x.asString.get.toDouble
                else
                  x.asNumber.get.toDouble
              }))
        case None                       => Left(DecodingFailure("Unable to parse the Map[String, Double]", Nil))
      }
    }
  }

  implicit val decodeDataSampl: Decoder[DataSampl] = new Decoder[`DataSampl`] {
    final def apply(c: HCursor): Decoder.Result[DataSampl] = {
      val tsResult = c.downArray.as[Long]
      val rightCurs = c.downArray.right
      if (rightCurs.focus.get.isObject) {
        for { timestamp <- tsResult
              buckets   <- rightCurs.as[Map[String, Double]]
              } yield {
          HistSampl(timestamp, buckets)
        }
      } else {
        for { timestamp <- tsResult
              value     <- rightCurs.as[String] } yield {
          Sampl(timestamp, value.toDouble)
        }
      }
    }
  }


  implicit val decodeAggregate: Decoder[AggregateResponse] = new Decoder[AggregateResponse] {
    final def apply(c: HCursor): Decoder.Result[AggregateResponse] = {
      val functionName = c.downField("function").as[String] match {
        case Right(fn) => fn
        case Left(ex) => throw ex
      }

     val aggregateSamples = functionName match {
        case Avg.entryName                 => c.downField ("aggregateValues").as[List[AvgSampl]]
        case QueryFunctionConstants.stdVal => c.downField("aggregateValues").as[List[StdValSampl]]
      }

      for {
       sample <- aggregateSamples
      } yield  AggregateResponse(functionName, sample)
    }
  }

  implicit val decodeQueryStatistics: Decoder[QueryStatistics] = new Decoder[QueryStatistics] {
    final def apply(c: HCursor): Decoder.Result[QueryStatistics] = {
      for {
        group             <- c.downField("group").as[Seq[String]]
        timeSeriesScanned <- c.downField("timeSeriesScanned").as[Long]
        dataBytesScanned  <- c.downField("dataBytesScanned").as[Long]
        resultBytes       <- c.downField("resultBytes").as[Long]
        cpuNanos          <- c.downField("cpuNanos").as[Option[Long]] // option to be backward compatible
      } yield {
        QueryStatistics(group, timeSeriesScanned, dataBytesScanned, resultBytes, cpuNanos.getOrElse(0))
      }
    }
  }

  implicit val decodeErrorResponse: Decoder[ErrorResponse] = new Decoder[ErrorResponse] {
    final def apply(c: HCursor): Decoder.Result[ErrorResponse] = {
      for {
        errorType <- c.downField("errorType").as[String]
        error     <- c.downField("error").as[String]
        status    <- c.downField("status").as[String]
        queryStats <- c.downField("queryStats").as[Option[Seq[QueryStatistics]]]
      } yield {
        ErrorResponse(errorType, error, status, queryStats)
      }
    }
  }
}
