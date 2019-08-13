package filodb.query

import io.circe.{Decoder, Encoder, HCursor, Json}

object PromCirceSupport {
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
}