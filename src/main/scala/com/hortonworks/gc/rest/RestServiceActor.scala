package com.hortonworks.gc.rest

import akka.actor.Actor
import akka.event.slf4j.SLF4JLogging
import com.hortonworks.gc.domain._
import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import com.hortonworks.gc.rest.LivyRestClient.StatementError
import com.hortonworks.gc.service.{LivyRestClientService, ScalableLivyRestClientService}
import net.liftweb.json.Serialization._
import net.liftweb.json.{DateFormat, Formats}
import spray.can.Http

import scala.Some
import spray.http._
import spray.httpx.unmarshalling._
import spray.routing._
import spray.can.server.Stats
import spray.http._

import spray.httpx.marshalling.Marshaller

/**
  * REST Service actor.
  */
class RestServiceActor extends Actor with RestService {

  implicit def actorRefFactory = context


  def receive = runRoute(rest)
}

/**
  * REST Service
  */
trait RestService extends HttpService with SLF4JLogging {

  val livyRestClientService = ScalableLivyRestClientService

  implicit val executionContext = actorRefFactory.dispatcher

  implicit val liftJsonFormats = new Formats {
    val dateFormat = new DateFormat {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")

      def parse(s: String): Option[Date] =
        try {
          Some(sdf.parse(s))
        } catch {
          case e: Exception => None
        }

      def format(d: Date): String = sdf.format(d)
    }
  }

  implicit val string2Date = new FromStringDeserializer[Date] {
    def apply(value: String) = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      try Right(sdf.parse(value))
      catch {
        case e: ParseException => {
          Left(
            MalformedContent("'%s' is not a valid Date value" format (value),
                             e))
        }
      }
    }
  }

  implicit val customRejectionHandler = RejectionHandler {
    case rejections =>
      mapHttpResponse { response =>
        response.withEntity(
          HttpEntity(ContentType(MediaTypes.`application/json`),
                     write(Map("error" -> response.entity.asString))))
      } {
        RejectionHandler.Default(rejections)
      }
  }

  val rest = respondWithMediaType(MediaTypes.`application/json`) {
      path("insrun") {
        get {
          parameters('sessionId.as[Int] ?, 'statement.as[String] ?).as(SparkStatement) {
            sparkStatement: SparkStatement  =>
            { ctx: RequestContext =>
              handleRequest(ctx) {
                log.warn("Interactive Spark Run Command: %s".format(
                  sparkStatement))
                val startTime = System.nanoTime

                val result = livyRestClientService.runCommand(sparkStatement)
                val endTime = System.nanoTime
                val timeTaken = (endTime - startTime).toDouble / (1 * 1000000000.0)
                log.warn(s"Average time taken in ${sparkStatement.sessionId} runs: $timeTaken seconds")

                result
              }
            }
          }
        }
      } ~
      path("closeConnection" ) {
        get { ctx: RequestContext =>
          handleRequest(ctx) {
            log.debug("Finally Closing Connection")
            livyRestClientService.closeConnection
          }
        }
      } ~
        path("createLivyContainer"/ IntNumber ) {
          numberOfExecutor =>
          get { ctx: RequestContext =>
            handleRequest(ctx) {
              log.debug("Create and Start Livy Container")
              livyRestClientService.createLivyContainer(numberOfExecutor)
            }
          }
        }~
        path("getAllIdleLivySessionIds" ) {
          get { ctx: RequestContext =>
            handleRequest(ctx) {
              log.debug("Get All Idle Livy Session ID's")
              livyRestClientService.getAllIdleLivySessionIds
            }
          }
        }~
        path("initSession") {
          get {
            parameters('sessionId.as[Int] ?, 'statement.as[String] ?).as(SparkStatement) {
              sparkStatement: SparkStatement  =>
              { ctx: RequestContext =>
                handleRequest(ctx) {
                  log.debug("Initialize Spark Imports for the Session : %s".format(
                    sparkStatement))
                  livyRestClientService.initSparkStatement(sparkStatement)
                }
              }
            }
          }
        }

  }

  implicit val statsMarshaller: Marshaller[Stats] =
    Marshaller.delegate[Stats, String](ContentTypes.`text/plain`) { stats =>
      "Uptime                : " + stats.uptime + '\n' +
        "Total requests        : " + stats.totalRequests + '\n' +
        "Open requests         : " + stats.openRequests + '\n' +
        "Max open requests     : " + stats.maxOpenRequests + '\n' +
        "Total connections     : " + stats.totalConnections + '\n' +
        "Open connections      : " + stats.openConnections + '\n' +
        "Max open connections  : " + stats.maxOpenConnections + '\n' +
        "Requests timed out    : " + stats.requestTimeouts + '\n'
    }


  def statsPresentation(s: Stats) = HttpResponse(
    entity = HttpEntity(
      <html>
        <body>
          <h1>HttpServer Stats</h1>
          <table>
            <tr><td>uptime:</td><td>{s.uptime.formatted("")}</td></tr>
            <tr><td>totalRequests:</td><td>{s.totalRequests}</td></tr>
            <tr><td>openRequests:</td><td>{s.openRequests}</td></tr>
            <tr><td>maxOpenRequests:</td><td>{s.maxOpenRequests}</td></tr>
            <tr><td>totalConnections:</td><td>{s.totalConnections}</td></tr>
            <tr><td>openConnections:</td><td>{s.openConnections}</td></tr>
            <tr><td>maxOpenConnections:</td><td>{s.maxOpenConnections}</td></tr>
            <tr><td>requestTimeouts:</td><td>{s.requestTimeouts}</td></tr>
          </table>
        </body>
      </html>.toString()
    )
  )

  /**
    * Handles an incoming request and create valid response for it.
    *
    * @param ctx         request context
    * @param successCode HTTP Status code for success
    * @param action      action to perform
    */
  protected def handleRequest(
      ctx: RequestContext,
      successCode: StatusCode = StatusCodes.OK)(action: => Either[_,_]) {
    action match {
      case Left(result: Object) =>
        ctx.complete(successCode, write(result))

      case Right(error: Failure) =>
        ctx.complete(
          error.getStatusCode,
          net.liftweb.json.Serialization.write(Map("error" -> error.message)))

      case _ =>
        ctx.complete(StatusCodes.InternalServerError)
    }
  }
}
