package filodb.query

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._

object PromCirceSupport {
  // necessary to encode sample in promql response as an array with long and double value as string
  // Specific encoders for *Sampl types
  implicit val encodeSampl: Encoder[DataSampl] = Encoder.instance {
    case s @ Sampl(t, v)     => Json.fromValues(Seq(t.asJson, v.toString.asJson))
    case h @ HistSampl(t, b) => Json.fromValues(Seq(t.asJson, b.asJson))
    case m @ MetadataSampl(v) => Json.fromValues(Seq(v.asJson))
  }

  implicit val decodeFoo: Decoder[DataSampl] = new Decoder[DataSampl] {
    final def apply(c: HCursor): Decoder.Result[DataSampl] = {
      val tsResult = c.downArray.as[Long]
      val rightCurs = c.downArray.right
      if (rightCurs.focus.get.isObject) {
        for { timestamp <- tsResult
              buckets   <- rightCurs.as[Map[String, Double]] } yield {
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
}
