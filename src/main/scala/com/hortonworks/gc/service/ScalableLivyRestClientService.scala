package com.hortonworks.gc.service

import java.net.URI
import java.util.concurrent._
import javax.servlet.http.HttpServletResponse

import akka.event.slf4j.SLF4JLogging
import com.cloudera.livy.client.common.HttpMessages.SessionInfo
import com.cloudera.livy.sessions.SessionKindModule
import com.cloudera.livy.{LivyClient, LivyClientBuilder}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hortonworks.gc.domain.SparkStatement
import com.hortonworks.gc.rest.LivyRestClient
import com.hortonworks.gc.rest.LivyRestClient.StatementError
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig}
import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer
import scala.util.Either

//import scalaj.collection.Imports._
//import scala.collection.JavaConversions._

object ScalableLivyRestClientService extends SLF4JLogging{

  class SessionList {
    val from: Int = -1
    val total: Int = -1
    val sessions: List[SessionInfo] = Nil
  }

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  val livyEndpoint = "http://usdf23v0378.mrshmc.com:9888"

  //val livyEndpoint = "http://cssc0.field.hortonworks.com:9888"

  //val executorService = Executors.newFixedThreadPool(10)

  val httpClientConfig =
    new AsyncHttpClientConfig.Builder()
      .setConnectTimeout(5 * 60 * 1000)
      .setReadTimeout(5 * 60 * 1000)
      .setWebSocketTimeout(5 * 60 * 1000)
      .setRequestTimeout(10 * 60 * 1000)
      .setPooledConnectionIdleTimeout(10 * 60 * 1000)
      //.setExecutorService(executorService)
      .setMaxConnections(50)
      .build()

  val httpClient: AsyncHttpClient = new AsyncHttpClient(httpClientConfig)
  val livyRestClient: LivyRestClient =
    new LivyRestClient(httpClient, livyEndpoint)

  val defaultSparkStatement = "val sparkVersion = sc.version"

  def runCommand(
      sparkStatement: SparkStatement): Either[String, StatementError] = {

    val interactiveSession = livyRestClient.connectSession(sparkStatement.sessionId.getOrElse(0))

    interactiveSession
      .run(sparkStatement.code.getOrElse(defaultSparkStatement))
      .result()
  }

  def closeConnection(): Either[String, StatementError] = {

    val list = sessionList()

    // get the livy session state by calling the rest api

      for (sessionInfo <- list.sessions) {
        if (sessionInfo.state.toLowerCase == "idle"
          || sessionInfo.state.toLowerCase == "starting"
          || sessionInfo.state.toLowerCase == "running") {

          // Wait till the session is IDLE
          livyRestClient.connectSession(sessionInfo.id).stop()
        }

      }

    if (httpClient != null)
      httpClient.close()

    Left("""Finally All Done""")
  }

  def createLivySession(): Int = {

    val list = sessionList()

    var newSessionId = 0

    // get the livy session state by calling the rest api
    val idleSessionId: Int = {
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

  def getAllIdleLivySessionIds(): Either[List[Int], StatementError] = {

    val list = sessionList()

    val idleSessionList = new ListBuffer[Int]()

    var newSessionId = 0

    // get the livy session state by calling the rest api
    val idleSessionIds: List[Int] = {
      for (sessionInfo <- list.sessions) {
        if (sessionInfo.state.toLowerCase == "idle"
          || sessionInfo.state.toLowerCase == "starting"
          || sessionInfo.state.toLowerCase == "running") {
          newSessionId = sessionInfo.id
          // Wait till the session is IDLE
          livyRestClient.connectSession(newSessionId).verifySessionIdle()
          idleSessionList.+=:(newSessionId)
        }

      }
      idleSessionList.toList
    }

    Left(idleSessionIds)
  }


  def initSparkStatementForUsecase4(sparkStatement: SparkStatement): Either[String, StatementError] = {

    val sessionId = sparkStatement.sessionId.getOrElse(0)

    val interactiveSession = livyRestClient.connectSession(sessionId)

    interactiveSession
      .run("val sparkVersion = sc.version")
      .result()
      .left
      .foreach(println(_))
    println(s" Valid session ID $sessionId")

    interactiveSession
      .run(
        "import org.apache.spark.sql.SparkSession\n" +
          "import org.apache.spark.sql.SQLContext\nimport org.apache.hadoop.conf.Configuration\n" +
          "import org.apache.spark.rdd.RDD\nimport org.apache.spark.sql.{Row, SparkSession}\n" +
          "import org.geotools.data.{DataStoreFinder, Query}\nimport org.geotools.factory.CommonFactoryFinder\n" +
          "import org.geotools.filter.text.ecql.ECQL\nimport org.locationtech.geomesa.hbase.data.HBaseDataStore\n" +
          "import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}\n" +
          "import org.opengis.feature.simple.SimpleFeature")
      .result()
      .left

    interactiveSession
      .run(
        "val sparkSession = SparkSession.builder().appName(\"testSpark\").config(\"spark.sql.crossJoin.enabled\", \"true\").config(\"zookeeper.znode.parent\", \"/hbase-unsecure\").config(\"spark.sql.autoBroadcastJoinThreshold\", 1024*1024*200).getOrCreate()")
      .result()
      .left
      .foreach(println(_))
    interactiveSession
      .run(
        "val dataFrame = sparkSession.read.format(\"geomesa\").options(Map(\"bigtable.table.name\" -> \"site_exposure_1M\")).option(\"geomesa.feature\", \"siteexposure_event\").load()")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        "val siteLossAnalyzFeatureTypeName = \"sitelossanalyzevent\"\nval featureTypeName = \"siteexposure_event\"\nval geom = \"geom\"\n \ndataFrame.createOrReplaceTempView(featureTypeName)")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        "  val dataFrameSiteLossAnalyz = sparkSession.read.format(\"geomesa\").options(Map(\"bigtable.table.name\" -> \"site_loss_analysis_1M\")).option(\"geomesa.feature\", siteLossAnalyzFeatureTypeName).load()")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        "dataFrameSiteLossAnalyz.createOrReplaceTempView(siteLossAnalyzFeatureTypeName)\n")
      .result()
      .left
      .foreach(println(_))


    log.warn("All statements are Initalized in the Livy Session ")

    Left(s"All Imports are Done for the session $sessionId")

  }


  def initSparkStatementForUsecase5(sparkStatement: SparkStatement): Either[String, StatementError] = {

    val sessionId = sparkStatement.sessionId.getOrElse(0)

    val interactiveSession = livyRestClient.connectSession(sessionId)

    interactiveSession
      .run("val sparkVersion = sc.version")
      .result()
      .left
      .foreach(println(_))
    println(s" Valid session ID $sessionId")

    interactiveSession
      .run("val sc = sparkSession.sparkContext\nval sqlContext = new SQLContext(sc)\nsqlContext.udf.register(\"WIDTH_BUCKET_1\" , com.hortonworks.gc.udf.WidthBucket.widthBucket _)   \n")
      .result()
      .left

    interactiveSession
      .run(
      " val sqlQuery = \"SELECT MAX((((se.cov1val + se.cov2val) + se.cov3val) + se.cov4val)) AS max_10, se.portfolio_id as portfolio_id, se.site_id as site_id  FROM siteexposure_event se  GROUP BY se.portfolio_id, se.site_id   \"")
      .result()
      .left
      .foreach(println(_))
    interactiveSession
      .run(
        " val resultDataFrame = sparkSession.sql(sqlQuery)\n    resultDataFrame.createOrReplaceTempView(\"myview\")\n   ")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        " val sqlQuery1 = \"SELECT max_10 , WIDTH_BUCKET_1(max_10, 150.000000, 3990682000.000000, 99) AS BIN, portfolio_id, site_id FROM myview \"")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        "  val dataFrameSiteLossAnalyz = sparkSession.read.format(\"geomesa\").options(Map(\"bigtable.table.name\" -> \"site_loss_analysis_1M\")).option(\"geomesa.feature\", siteLossAnalyzFeatureTypeName).load()")
      .result()
      .left
      .foreach(println(_))

    interactiveSession
      .run(
        " val resultDataFrame1 = sparkSession.sql(sqlQuery1)\n    resultDataFrame1.createOrReplaceTempView(\"myview1\")")
      .result()
      .left
      .foreach(println(_))


    log.warn("All statements are Initalized for usecase 5 in the Livy Session ")

    Left(s"All Imports are Done for the session $sessionId")

  }

  private def sessionList(): SessionList = {
    val response =
      httpClient.prepareGet(s"$livyEndpoint/sessions/").execute().get()
    if (response.getStatusCode != HttpServletResponse.SC_OK) {
      throw new RuntimeException("Unable to get session from the Session")
    }
    mapper.readValue(response.getResponseBodyAsStream, classOf[SessionList])
  }

  def closeLivySession(livyUrl: String, livySessionId: Int): Unit = {

    val createLivySessionRequest = new HttpDelete(
      livyUrl + s"/sessions/$livySessionId")
    createLivySessionRequest.setHeader("Content-type", "application/json")
    createLivySessionRequest.setHeader("X-Requested-By", "spark")

    val response = (new DefaultHttpClient).execute(createLivySessionRequest) // Execute our request

    // Finally, print out the results

    val responseBody = EntityUtils.toString(response.getEntity)

    println(responseBody)

  }

  def createLivyContainer(numOfContainer :Int): Either[String, StatementError] = {
    val requestBody = Map(
//                          "driverMemory" -> "1g",
//                          "numExecutors" -> 10,
//                          "executorMemory" -> "1g",
//                          "executorCores" -> 1,
      // "kind" -> "spark",
      "conf" -> Map(
        "spark.driver.memory" -> "1g",
        "spark.yarn.driver.memoryOverhead" -> "256",
        s"spark.executor.instances" -> s"$numOfContainer",
        "spark.executor.memory" -> "1g",
        "spark.yarn.executor.memoryOverhead" -> "256",
        "spark.executor.cores" -> "5",
        "spark.memory.fraction" -> "0.2",
        "spark.jars.excludes" -> "org.scala-lang:scala-reflect, org.apache.spark:spark-tags_2.11"
        //"spark.jars" -> "hdfs://csma0.field.hortonworks.com:8020/tmp/geomesa/geomesa-hbase_2.11-1.3.2/dist/spark/geomesa-hbase-spark-runtime_2.11-1.3.2.jar"
      ),
      "kind" -> "spark",
      "name" -> "Livy Interactive Session ",
      "files" -> List(
        "hdfs:///tmp/etc/hbase/conf/hbase-site.xml"),
      "jars" -> List(
        "hdfs:///tmp/geomesa/geomesa-hbase_2.11-1.3.3/dist/spark/manuallychanges/geomesa-hbase-spark-runtime_2.11-1.3.3.jar" ,
        "hdfs:///tmp/geoanalytics-1.0-SNAPSHOT.jar")
    )

    println(mapper.writeValueAsString(requestBody))

    val response = httpClient
      .preparePost(s"$livyEndpoint/sessions/")
      .addHeader("Content-type", "application/json")
      .addHeader("X-Requested-By", "spark")
      .setBody(mapper.writeValueAsString(requestBody))
      .execute()
      .get(2, TimeUnit.MINUTES)

    if (response.getStatusCode != HttpServletResponse.SC_CREATED) {
      throw new RuntimeException("Unable to get session from the Session")
    }

    println("Cotainer")

    Left("Container Created " + response.getStatusCode)

  }
  // ------ Unused Methods Just use for reference  !!! No Clean UP .. shame on me
  def main(args: Array[String]): Unit = {

    try {

      val sessionId = 0
      val interactiveSession = livyRestClient.connectSession(sessionId)

      interactiveSession
        .run(
          "val sparkSession = SparkSession.builder().appName(\"testSpark\").config(\"spark.sql.crossJoin.enabled\", \"true\").config(\"zookeeper.znode.parent\", \"/hbase-unsecure\").config(\"spark.sql.autoBroadcastJoinThreshold\", 1024*1024*200).getOrCreate()")
        .result()
        .left
        .foreach(println(_))
      interactiveSession
        .run(
          "val dataFrame = sparkSession.read.format(\"geomesa\").options(Map(\"bigtable.table.name\" -> \"siteexposure_1M\")).option(\"geomesa.feature\", \"event\").load()")
        .result()
        .left
        .foreach(println(_))
      println(s" Valid session ID $sessionId")
      interactiveSession.run("dataFrame.show(1)").result().left
      println(s" Valid session ID $sessionId")

    } finally {
      // Stop the ning HTTP Client
      closeConnection()

      println("finally all done ")
    }

    def createClient(uri: String): LivyClient = {
      val props =
        Map(
          "spark.sql.crossJoin.enabled" -> "true",
          "zookeeper.znode.parent" -> "/hbase-unsecure",
          "spark.sql.autoBroadcastJoinThreshold" -> "1024*1024*200",
          "livy.spark.driver.memory" -> "1g",
          "livy.spark.yarn.driver.memoryOverhead" -> "256",
          "livy.spark.executor.instances" -> "10",
          "livy.spark.executor.memory" -> "1g",
          "livy.spark.yarn.executor.memoryOverhead" -> "256",
          "livy.spark.executor.cores" -> "1",
          "livy.spark.memory.fraction" -> "0.2"
        )

      /*
       "spark.driver.memory": "1g",
          "spark.yarn.driver.memoryOverhead": "256",
          "spark.executor.instances": "20",
          "spark.executor.memory": "1g",
          "spark.yarn.executor.memoryOverhead": "256",
          "spark.executor.cores": "1",
          "spark.memory.fraction": "0.2"
       */
      new LivyClientBuilder()
        .setURI(new URI(uri))
        .setConf("spark.driver.memory", "1g")
        .setConf("spark.executor.instances", "10")
        .setConf("spark.executor.memory", "1g")
        .build()
    }

  }


}
