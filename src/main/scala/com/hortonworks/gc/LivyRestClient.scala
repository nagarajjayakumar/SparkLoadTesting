package com.hortonworks.gc

import java.util.regex.Pattern
import javax.servlet.http.HttpServletResponse

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Either, Left, Right}
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.Response
import com.cloudera.livy.sessions.{Kind, SessionKindModule, SessionState}
import main.scala.com.hortonworks.gc.GeoMesaBasicSimulation.{httpClient, mapper}
import org.scalatest.concurrent.Eventually._

import scala.collection.JavaConverters._



object LivyRestClient {

  private val BATCH_TYPE = "batches"
  private val INTERACTIVE_TYPE = "sessions"

  object AppInfo {
    val DRIVER_LOG_URL_NAME = "driverLogUrl"
    val SPARK_UI_URL_NAME = "sparkUiUrl"
  }

  case class AppInfo(var driverLogUrl: Option[String] = None, var sparkUiUrl: Option[String] = None) {
    import AppInfo._
    def asJavaMap: java.util.Map[String, String] =
      Map(DRIVER_LOG_URL_NAME -> driverLogUrl.orNull, SPARK_UI_URL_NAME -> sparkUiUrl.orNull).asJava
  }

  // TODO Define these in production code and share them with test code.
  @JsonIgnoreProperties(ignoreUnknown = true)
  private case class StatementResult(id: Int, state: String, output: Map[String, Any])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class StatementError(ename: String, evalue: String, stackTrace: Seq[String])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class SessionSnapshot(
                              id: Int,
                              appId: Option[String],
                              state: String,
                              appInfo: AppInfo,
                              log: IndexedSeq[String])
}

class LivyRestClient(val httpClient: AsyncHttpClient, val livyEndpoint: String) {
  import LivyRestClient._

  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  class Session(val id: Int, sessionType: String) {
    val url: String = s"$livyEndpoint/$sessionType/$id"



    def snapshot(): SessionSnapshot = {
      val r = httpClient.prepareGet(url)
        .addHeader("Content-type", "application/json")
        .addHeader("X-Requested-By", "spark")
        .execute().get()
      assertStatusCode(r, HttpServletResponse.SC_OK)

      mapper.readValue(r.getResponseBodyAsStream, classOf[SessionSnapshot])
    }

    def stop(): Unit = {
      httpClient.prepareDelete(url).
          addHeader("Content-type", "application/json").
          addHeader("X-Requested-By", "spark").
          execute().get()

      eventually(timeout(30 seconds), interval(1 second)) {
        verifySessionDoesNotExist()
      }
    }

    def verifySessionState(state: SessionState): Unit = {
      verifySessionState(Set(state))
    }

    def verifySessionState(states: Set[SessionState]): Unit = {
      val t = 2.minutes
      val strStates = states.map(_.toString)
      // Travis uses very slow VM. It needs a longer timeout.
      // Keeping the original timeout to avoid slowing down local development.
      eventually(timeout(t), interval(1 second)) {
        val s = snapshot().state
        assert(strStates.contains(s), s"Session $id state $s doesn't equal one of $strStates")
      }
    }

    def verifySessionDoesNotExist(): Unit = {
      val r = httpClient.prepareGet(url).
        addHeader("Content-type", "application/json").
        addHeader("X-Requested-By", "spark").execute().get()
      assertStatusCode(r, HttpServletResponse.SC_NOT_FOUND)
    }
  }

  class BatchSession(id: Int) extends Session(id, BATCH_TYPE) {
    def verifySessionDead(): Unit = verifySessionState(SessionState.Dead())
    def verifySessionRunning(): Unit = verifySessionState(SessionState.Running())
    def verifySessionSuccess(): Unit = verifySessionState(SessionState.Success())
  }

  class InteractiveSession(id: Int) extends Session(id, INTERACTIVE_TYPE) {
    class Statement(code: String) {
      val stmtId = {
        val requestBody = Map("code" -> code)
        val r = httpClient.preparePost(s"$url/statements")
          .addHeader("Content-type", "application/json")
          .addHeader("X-Requested-By", "spark")
          .setBody(mapper.writeValueAsString(requestBody))
          .execute()
          .get()
        assertStatusCode(r, HttpServletResponse.SC_CREATED)

        val newStmt = mapper.readValue(r.getResponseBodyAsStream, classOf[StatementResult])
        newStmt.id
      }

      final def result(): Either[String, StatementError] = {
        eventually(timeout(1 minute), interval(1 second)) {
          val r = httpClient.prepareGet(s"$url/statements/$stmtId")
            .addHeader("Content-type", "application/json")
            .addHeader("X-Requested-By", "spark")
            .execute()
            .get()
          assertStatusCode(r, HttpServletResponse.SC_OK)

          val newStmt = mapper.readValue(r.getResponseBodyAsStream, classOf[StatementResult])
          assert(newStmt.state == "available", s"Statement isn't available: ${newStmt.state}")

          val output = newStmt.output
          output.get("status") match {
            case Some("ok") =>
              val data = output("data").asInstanceOf[Map[String, Any]]
              var rst = data.getOrElse("text/plain", "")
              val magicRst = data.getOrElse("application/vnd.livy.table.v1+json", null)
              if (magicRst != null) {
                rst = mapper.writeValueAsString(magicRst)
              }
              Left(rst.asInstanceOf[String])
            case Some("error") => Right(mapper.convertValue(output, classOf[StatementError]))
            case Some(status) =>
              throw new IllegalStateException(s"Unknown statement $stmtId status: $status")
            case None =>
              throw new IllegalStateException(s"Unknown statement $stmtId output: $newStmt")
          }
        }
      }

      def verifyResult(expectedRegex: String): Unit = {
        result() match {
          case Left(result) =>
            if (expectedRegex != null) {
              matchStrings(result, expectedRegex)
            }
          case Right(error) =>
            assert(false, s"Got error from statement $stmtId $code: ${error.evalue}")
        }
      }

      def verifyError(
                       ename: String = null, evalue: String = null, stackTrace: String = null): Unit = {
        result() match {
          case Left(result) =>
            assert(false, s"Statement $stmtId `$code` expected to fail, but succeeded.")
          case Right(error) =>
            val remoteStack = Option(error.stackTrace).getOrElse(Nil).mkString("\n")
            Seq(error.ename -> ename, error.evalue -> evalue, remoteStack -> stackTrace).foreach {
              case (actual, expected) if expected != null => matchStrings(actual, expected)
              case _ =>
            }
        }
      }

      private def matchStrings(actual: String, expected: String): Unit = {
        val regex = Pattern.compile(expected, Pattern.DOTALL)
        assert(regex.matcher(actual).matches(), s"$actual did not match regex $expected")
      }
    }

    def run(code: String): Statement = { new Statement(code) }

    def runFatalStatement(code: String): Unit = {
      val requestBody = Map("code" -> code)
      val r = httpClient.preparePost(s"$url/statements")
        .addHeader("Content-type", "application/json")
        .addHeader("X-Requested-By", "spark")
        .setBody(mapper.writeValueAsString(requestBody))
        .execute()

      verifySessionState(SessionState.Dead())
    }

    def verifySessionIdle(): Unit = {
      verifySessionState(SessionState.Idle())
    }
  }



  def connectSession(id: Int): InteractiveSession = { new InteractiveSession(id) }

  private def start(sessionType: String, body: String): Int = {
    val r = httpClient.preparePost(s"$livyEndpoint/$sessionType")
      .addHeader("Content-type", "application/json")
      .addHeader("X-Requested-By", "spark")
      .setBody(body)
      .execute()
      .get()

    assertStatusCode(r, HttpServletResponse.SC_CREATED)

    val newSession = mapper.readValue(r.getResponseBodyAsStream, classOf[SessionSnapshot])
    newSession.id
  }

  private def assertStatusCode(r: Response, expected: Int): Unit = {
    def pretty(r: Response): String = {
      s"${r.getStatusCode} ${r.getResponseBody}"
    }
    assert(r.getStatusCode() == expected, s"HTTP status code != $expected: ${pretty(r)}")
  }
}