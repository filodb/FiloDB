package filodb.query

import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}

object PromCirceSupport {

  import io.circe.syntax._

  // necessary to encode sample in promql response as an array with long and double value as string
  implicit val encodeSampl: Encoder[Sampl] = new Encoder[Sampl] {
    final def apply(a: Sampl): Json = Json.arr(Json.fromLong(a.timestamp), Json.fromString(a.value.toString))
  }

  implicit val decodeFoo: Decoder[Sampl] = new Decoder[Sampl] {
    final def apply(c: HCursor): Decoder.Result[Sampl] = {
      for {timestamp <- c.downArray.as[Long].right
           value <- c.downArray.right.as[String].right
      } yield {
        Sampl(timestamp, value.toDouble)
      }
    }
  }

  implicit val decodeResult: Decoder[Result] = new Decoder[Result] {
    override def apply(c: HCursor): Decoder.Result[Result] = {
      val valuesDecoder = c.downField("values").as[Seq[Sampl]] match {
        case Left(_) => Right[DecodingFailure, Seq[Sampl]](Seq.empty)
        case Right(s) => Right[DecodingFailure, Seq[Sampl]](s)
      }
      val valueDecoder = c.downField("value").as[Sampl] match {
        case Left(_) => Right[DecodingFailure, Sampl](Sampl(-1, -1))
        case Right(s) => Right[DecodingFailure, Sampl](s)
      }
      for {
        metric <- c.downField("metric").as[Map[String, String]].right
        values <- valuesDecoder.right
        value <- valueDecoder.right
      } yield {
        Result(metric, values, value)
      }
    }
  }

  implicit val encodeResult: Encoder[Result] = new Encoder[Result] {
    final def apply(a: Result): Json = {
      if (a.values.nonEmpty) {
        Json.obj(("metric", a.metric.asJson), ("values", a.values.asJson))
      } else {
        Json.obj(("metric", a.metric.asJson), ("value", a.value.asJson))
      }
    }
  }

}