package filodb.query

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._

import filodb.query.AggregationOperator.Avg

object PromCirceSupport {
  // necessary to encode sample in promql response as an array with long and double value as string
  // Specific encoders for *Sampl types
  implicit val encodeSampl: Encoder[DataSampl] = Encoder.instance {
    case s @ Sampl(t, v)     => Json.fromValues(Seq(t.asJson, v.toString.asJson))
    case h @ HistSampl(t, b) => Json.fromValues(Seq(t.asJson, b.asJson))
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

  implicit val decodeRemoteErrorResponse: Decoder[RemoteErrorResponse] = new Decoder[RemoteErrorResponse] {
    final def apply(c: HCursor): Decoder.Result[RemoteErrorResponse] = {
      for {
        status    <- c.downField("status").as[String]
        errorType <- c.downField("errorType").as[String]
        error     <- c.downField("error").as[String]
      } yield {
        RemoteErrorResponse(status, errorType, error)
      }
    }
  }
}
