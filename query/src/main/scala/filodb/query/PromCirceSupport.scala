package filodb.query

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._

object PromCirceSupport {
  import cats.syntax.either._
  // necessary to encode sample in promql response as an array with long and double value as string
  // Specific encoders for *Sampl types
  implicit val encodeSampl: Encoder[DataSampl] = Encoder.instance {
    case s @ Sampl(t, v)     => Json.fromValues(Seq(t.asJson, v.toString.asJson))
    case h @ HistSampl(t, b) => Json.fromValues(Seq(t.asJson, b.asJson))
    case h @ AvgSampl(t, v, c) => Json.fromValues(Seq(t.asJson, v.toString.asJson, c.toString.asJson))
    case m @ MetadataSampl(v) => Json.fromValues(Seq(v.asJson))
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

  implicit val decodeSample: Decoder[Sampl] = new Decoder[Sampl] {
    final def apply(c: HCursor): Decoder.Result[Sampl] = {
      for { timestamp <- c.downArray.as[Long]
            value <- c.downArray.right.as[String]} yield {
        Sampl(timestamp, value.toDouble)
      }
    }
  }

  implicit val decodeHistSampl: Decoder[HistSampl] = new Decoder[HistSampl] {
    final def apply(c: HCursor): Decoder.Result[HistSampl] = {
      val tsResult = c.downArray.as[Long]
      val rightCurs = c.downArray.right
        for { timestamp <- tsResult
              buckets <- rightCurs.as[Map[String, Double]]
             } yield {
          HistSampl(timestamp, buckets)
        }
    }
  }

  implicit val decodeFoo: Decoder[DataSampl] = new Decoder[DataSampl] {
    final def apply(c: HCursor): Decoder.Result[DataSampl] = {
      c.values.get.toList.size match
      {
        case 3 =>  c.as[AvgSampl]
        case 2 =>  c.as[Sampl]
        case _ =>  c.as[HistSampl] // To do change default condition
      }
    }
  }
}
