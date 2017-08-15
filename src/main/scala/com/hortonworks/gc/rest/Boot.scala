package com.hortonworks.gc.rest

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.routing.RoundRobinPool
import com.hortonworks.gc.config.Configuration
import spray.can.Http

object Boot extends App with Configuration {

  // create an actor system for application
  implicit val system = ActorSystem("njGatling_Rest_Service")

  // create and start rest service actor
  //val restService = system.actorOf(Props[RestServiceActor], "njGatling_Rest_Service_Endpoint")

  val restService = system.actorOf(RoundRobinPool(10).props(Props[RestServiceActor]), "njGatling_Rest_Service_Endpoint")

  // start HTTP server with rest service actor as a handler
  IO(Http) ! Http.Bind(restService, serviceHost, servicePort)

}

