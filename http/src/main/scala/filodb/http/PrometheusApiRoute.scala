package filodb.http

import scala.concurrent.Future

import akka.actor.ActorRef
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, MediaTypes, StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.xerial.snappy.Snappy
import remote.RemoteStorage.ReadRequest

import filodb.coordinator.client.IngestionCommands.UnknownDataset
import filodb.coordinator.client.QueryCommands._
import filodb.core.{DatasetRef, SpreadChange, SpreadProvider}
import filodb.core.query.{PromQlQueryParams, QueryContext, TsdbQueryParams}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec.ExecPlan

class PrometheusApiRoute(nodeCoord: ActorRef, settings: HttpSettings)(implicit am: ActorMaterializer)
           extends FiloRoute with StrictLogging {

  import FailFastCirceSupport._
  import io.circe.generic.auto._
  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override DataSampl case class Encoder/Decoders.
  import PromCirceSupport._
  import filodb.coordinator.client.Client._
  import filodb.prometheus.query.PrometheusModel._

  val schemas = settings.filoSettings.schemas

  val route = pathPrefix( "promql" / Segment) { dataset =>
    // Path: /promql/<datasetName>/api/v1/query_range
    // Used to issue a promQL query for a time range with a `start` and `end` timestamp and at regular `step` intervals.
    // For more details, see Prometheus HTTP API Documentation
    // [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries)
    path( "api" / "v1" / "query_range") {
      get {
        parameter(('query.as[String], 'start.as[Double], 'end.as[Double], 'histogramMap.as[Boolean].?,
          'step.as[Int], 'explainOnly.as[Boolean].?, 'verbose.as[Boolean].?, 'spread.as[Int].?))
        { (query, start, end, histMap, step, explainOnly, verbose, spread) =>
          val logicalPlan = Parser.queryRangeToLogicalPlan(query, TimeStepParams(start.toLong, step, end.toLong))

          // No cross-cluster failure routing in this API, hence we pass empty config
          askQueryAndRespond(dataset, logicalPlan, explainOnly.getOrElse(false), verbose.getOrElse(false),
            spread, PromQlQueryParams(query, start.toLong, step.toLong, end.toLong), histMap.getOrElse(false))
        }
      }
    } ~
    // Path: /promql/<datasetName>/api/v1/query
    // Used to issue a promQL query for a single time instant `time`.  Can also be used to query raw data by issuing
    // a PromQL range expression. For more details, see Prometheus HTTP API Documentation
    // [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries)
    path( "api" / "v1" / "query") {
      get {
        parameter(('query.as[String], 'time.as[Double], 'explainOnly.as[Boolean].?, 'verbose.as[Boolean].?,
          'spread.as[Int].?, 'histogramMap.as[Boolean].?, 'step.as[Double].?))
        { (query, time, explainOnly, verbose, spread, histMap, step) =>
          val stepLong = step.map(_.toLong).getOrElse(0L)
          val logicalPlan = Parser.queryToLogicalPlan(query, time.toLong, stepLong)
          askQueryAndRespond(dataset, logicalPlan, explainOnly.getOrElse(false),
            verbose.getOrElse(false), spread, PromQlQueryParams(query, time.toLong, stepLong, time.toLong),
            histMap.getOrElse(false))
        }
      }
    } ~
    // Path: /promql/<datasetName>/api/v1/read
    // Used to extract raw data for integration with other TSDB systems.
    //  * Input: ReadRequest Protobuf
    //  * Output: ReadResponse Protobuf
    // See [Prometheus Remote Proto Def](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto)
    // for more details
    /*  Important Note: This Prometheus API should NOT be used for extracting raw data out from FiloDB at scale. Current
    implementation includes the same 'limit' settings that apply in the Akka Actor interface.
    Currently, it is designed and intended only for functional testing of query engine. */
    path( "api" / "v1" / "read") {
      post {
        extractDataBytes { data =>
          val bytesFut = data.runFold(ByteString.empty) { case (acc, b) => acc ++ b }
          val fut = bytesFut.flatMap { bytes =>
            // Would have ideally liked to have the unencoding driven by content encoding headers,
            // but Akka doesnt support snappy out of the box. Elegant solution is a TODO for later.
            val readReq = ReadRequest.parseFrom(Snappy.uncompress(bytes.toArray))
            val asks = toFiloDBLogicalPlans(readReq).map { logicalPlan =>
              asyncAsk(nodeCoord, LogicalPlan2Query(DatasetRef.fromDotString(dataset), logicalPlan),
                       settings.queryAskTimeout)
            }
            Future.sequence(asks)
          }
          onSuccess(fut) { case qr =>
            qr.find(!_.isInstanceOf[filodb.query.QueryResult]) match {
              case Some(qe: QueryError) => complete(toPromErrorResponse(qe))
              case Some(UnknownDataset) => complete(Codes.NotFound ->
                                           ErrorResponse("badQuery", s"Dataset $dataset is not registered"))
              case Some(a: Any)      => throw new IllegalStateException(s"Got $a as query response")
              case None              => val promQrs = qr.asInstanceOf[Seq[filodb.query.QueryResult]].map { r =>
                                          convertHistToPromResult(r, schemas.part)
                                        }
                                        val rrBytes = toPromReadResponse(promQrs)
                                        // Would have ideally liked to have the encoding driven by akka directives,
                                        // but Akka doesnt support snappy out of the box.
                                        // Elegant solution is a TODO for later.
                                        val body = ByteString(Snappy.compress(rrBytes))
                                        val entity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, body)
                                        complete(HttpResponse(entity = entity))
            }
          }
        }
      }
    }
  }

  private def askQueryAndRespond(dataset: String, logicalPlan: LogicalPlan, explainOnly: Boolean, verbose: Boolean,
                                 spread: Option[Int], tsdbQueryParams: TsdbQueryParams, histMap: Boolean) = {
    val spreadProvider: Option[SpreadProvider] = spread.map(s => StaticSpreadProvider(SpreadChange(0, s)))
    val command = if (explainOnly) {
      ExplainPlan2Query(DatasetRef.fromDotString(dataset), logicalPlan, QueryContext(tsdbQueryParams, spreadProvider))
    }
    else {
      LogicalPlan2Query(DatasetRef.fromDotString(dataset), logicalPlan, QueryContext(tsdbQueryParams, spreadProvider))
    }
    onSuccess(asyncAsk(nodeCoord, command, settings.queryAskTimeout)) {
      case qr: QueryResult => val translated = if (histMap) qr else convertHistToPromResult(qr, schemas.part)
                              complete(toPromSuccessResponse(translated, verbose))
      case qr: QueryError => complete(toPromErrorResponse(qr))
      case qr: ExecPlan => complete(toPromExplainPlanResponse(qr))
      case UnknownDataset => complete(Codes.NotFound ->
        ErrorResponse("badQuery", s"Dataset $dataset is not registered"))
    }
  }
}
