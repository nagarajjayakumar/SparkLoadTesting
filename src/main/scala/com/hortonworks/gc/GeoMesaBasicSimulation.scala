package main.scala.com.hortonworks.gc

import java.net.URI
import java.util

import com.cloudera.livy.{LivyClient, LivyClientBuilder}
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import com.google.gson.Gson
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import scala.collection.JavaConverters._
import com.cloudera.livy.client.common.HttpMessages.SessionInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.cloudera.livy.sessions.{SessionKindModule, SessionState}
import com.hortonworks.gc.LivyRestClient
import com.ning.http.client.AsyncHttpClient
import com.cloudera.livy.sessions._
import main.scala.com.hortonworks.gc.GeoMesaBasicSimulation.{livyClient, sessionId}

import scala.concurrent.duration._

//import scalaj.collection.Imports._
//import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import scala.collection.JavaConverters._
import scala.collection.JavaConverters._
import collection.JavaConversions._


object GeoMesaBasicSimulation  {


  class SessionList {
    val from: Int = -1
    val total: Int = -1
    val sessions: List[SessionInfo] = Nil
  }

  private var livyClient: LivyClient = _
  private var sessionId: Int = _
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  val livyEndpoint = "http://cssc0.field.hortonworks.com:8999"


  var httpClient: AsyncHttpClient = _
  var livyRestClient: LivyRestClient = _


  import javax.servlet.http.HttpServletResponse


  def main(args: Array[String]): Unit = {

    try {
      httpClient = new AsyncHttpClient()
      livyRestClient = new LivyRestClient(httpClient, livyEndpoint)

      // Create the livy client - nothing but creates the yarn containers with the given conf
      livyClient = createClient(livyEndpoint)

      val sessionId = createLivySession(livyEndpoint)

      val interactiveSession = livyRestClient.connectSession(sessionId)

      interactiveSession.run("val sparkVersion = sc.version").result().left.foreach(println(_))
      println(s" Valid session ID $sessionId")


      interactiveSession.run("import org.apache.spark.sql.SparkSession\n" +
        "import org.apache.spark.sql.SQLContext\nimport org.apache.hadoop.conf.Configuration\n" +
        "import org.apache.spark.rdd.RDD\nimport org.apache.spark.sql.{Row, SparkSession}\n" +
        "import org.geotools.data.{DataStoreFinder, Query}\nimport org.geotools.factory.CommonFactoryFinder\n" +
        "import org.geotools.filter.text.ecql.ECQL\nimport org.locationtech.geomesa.hbase.data.HBaseDataStore\n" +
        "import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}\n" +
        "import org.opengis.feature.simple.SimpleFeature").result().left

      interactiveSession.run("import org.apache.spark.sql.SparkSession").result().left.foreach(println(_))
      interactiveSession.run("import org.apache.spark.sql.SQLContext").result().left.foreach(println(_))
      interactiveSession.run("import org.apache.hadoop.conf.Configuration").result().left.foreach(println(_))
      interactiveSession.run("import org.apache.spark.rdd.RDD").result().left.foreach(println(_))
      interactiveSession.run("import org.apache.spark.sql.{Row, SparkSession}").result().left.foreach(println(_))
      interactiveSession.run("import org.geotools.data.{DataStoreFinder, Query}").result().left.foreach(println(_))
      interactiveSession.run("import org.geotools.factory.CommonFactoryFinder").result().left.foreach(println(_))
      interactiveSession.run("import org.geotools.filter.text.ecql.ECQL").result().left.foreach(println(_))
      interactiveSession.run("import org.locationtech.geomesa.hbase.data.HBaseDataStore").result().left.foreach(println(_))
      interactiveSession.run("import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}").result().left.foreach(println(_))
      interactiveSession.run("import org.opengis.feature.simple.SimpleFeature").result().left.foreach(println(_))

      println(s" Valid session ID $sessionId")


      interactiveSession.run("val dataFrame = sparkSession.read\n      .format(\"geomesa\")\n      .options(Map(\"bigtable.table.name\" -> \"siteexposure_1M\"))\n      .option(\"geomesa.feature\", \"event\")\n      .load()")

      interactiveSession.run("dataFrame.show(1)").result().left
      // Walid Idle session ID is

      println(s" Valid session ID $sessionId")

      // Stop the ning HTTP Client



      if(livyClient != null)
        livyClient.stop(true)

      livyRestClient.connectSession(sessionId).stop()

      println("all done ")


    }finally {
      // Stop the ning HTTP Client
      if(httpClient != null)
        httpClient.close()

      println("finally all done ")
    }

  }

  def createLivySession (livyUrl:String): Int = {

    val list = sessionList()

    var newSessionId = 0

    // get the livy session state by calling the rest api
    val idleSessionId : Int = {
      for (sessionInfo <- list.sessions) {
        if (sessionInfo.state.toLowerCase == "idle"
              || sessionInfo.state.toLowerCase == "starting"
                || sessionInfo.state.toLowerCase == "running") {
          newSessionId = sessionInfo.id
          // Wait till the session is IDLE
          livyRestClient.connectSession(newSessionId).verifySessionIdle()
          return newSessionId
        }

      }
      newSessionId
    }

    idleSessionId
  }

  def createClient(uri: String): LivyClient = {
    val props =
      Map("spark.sql.crossJoin.enabled" -> "true",
            "zookeeper.znode.parent" -> "/hbase-unsecure",
            "spark.sql.autoBroadcastJoinThreshold" -> "1024*1024*200",
            "livy.spark.driver.memory" ->"1g",
            "livy.spark.yarn.driver.memoryOverhead" -> "256",
            "livy.spark.executor.instances" -> "10",
            "livy.spark.executor.memory" -> "1g",
            "livy.spark.yarn.executor.memoryOverhead" -> "256",
            "livy.spark.executor.cores"-> "1",
            "livy.spark.memory.fraction"-> "0.2")

    /*
     "spark.driver.memory": "1g",
        "spark.yarn.driver.memoryOverhead": "256",
        "spark.executor.instances": "20",
        "spark.executor.memory": "1g",
        "spark.yarn.executor.memoryOverhead": "256",
        "spark.executor.cores": "1",
        "spark.memory.fraction": "0.2"
     */
    new LivyClientBuilder().setURI(new URI(uri)).
      setConf("spark.driver.memory","1g").
      setConf("spark.executor.instances" , "10").
      setConf("spark.executor.memory" , "1g").
      build()
  }


  private def sessionList(): SessionList = {
    val response = httpClient.prepareGet(s"$livyEndpoint/sessions/").execute().get()
    if(response.getStatusCode != HttpServletResponse.SC_OK){
      throw new RuntimeException("Unable to get session from the Session")
    }
    mapper.readValue(response.getResponseBodyAsStream, classOf[SessionList])
  }


  def closeLivySession (livyUrl:String, livySessionId : Int): Unit = {

    val createLivySessionRequest = new HttpDelete(livyUrl +s"/sessions/$livySessionId")
    createLivySessionRequest.setHeader("Content-type", "application/json")
    createLivySessionRequest.setHeader("X-Requested-By", "spark")

    val response = (new DefaultHttpClient).execute(createLivySessionRequest) // Execute our request

    // Finally, print out the results

    val responseBody = EntityUtils.toString(response.getEntity)

    println(responseBody)

  }


//  val userLog = csv("user_credentials.csv").circular
//
//
//  val scn = {
//    scenario(s"testing the testResource ( $host )")
//      .exec(
//        http("testRouteSim")
//          .get("/testRoute")
//          .check(status.is(200)
//          )
//      )
//  }
//
//  /*setUp(scn.inject(ramp(3 users) over (10 seconds)))
//        //Assert the output max time is less than 50ms and 95% of requests were successful
//        .assertions(global.responseTime.max.lessThan(50),global.successfulRequests.percent.greaterThan(95))*/
//
//  setUp(
//    scn.inject(
//      nothingFor(4 seconds), // 1
//      atOnceUsers(10), // 2
//      rampUsers(3) over (2 seconds), // 3
//      constantUsersPerSec(5) during (2 seconds), // 4
//      constantUsersPerSec(5) during (2 seconds) randomized, // 5
//      rampUsersPerSec(10) to 20 during (3 seconds), // 6
//      rampUsersPerSec(10) to 20 during (2 seconds) randomized, // 7
//      splitUsers(10) into (rampUsers(2) over (5 seconds)) separatedBy (2 seconds), // 8
//      splitUsers(10) into (rampUsers(2) over (5 seconds)) separatedBy atOnceUsers(
//        5), // 9
//      heavisideUsers(10) over (500 milliseconds) // 10
//    )
//    //Assert the output max time is less than 50ms and 95% of requests were successful
//  ).assertions(global.responseTime.max.lessThan(50),
//    global.successfulRequests.percent.greaterThan(95))
}
