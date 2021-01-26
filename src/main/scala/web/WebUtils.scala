package web

import java.net.InetAddress

import Clustering.ClusterListener
import akka.actor.{ActorSystem, Props}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.typesafe.config.ConfigFactory
import web.model.request.HelloWorldRequest

object WebUtils {

  def startActorMaster(): ActorSystem = {
    val localIpAddress = InetAddress.getLocalHost.getHostAddress
    val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.hostname = $localIpAddress
        akka.remote.netty.tcp.port = 2552
        akka.remote.artery.canonical.port = 2552
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka.tcp://Amber@$localIpAddress:2552" ]
        """).withFallback(ConfigFactory.load("clustered"))

    val system = ActorSystem("Amber",config)
    val info = system.actorOf(Props[ClusterListener],"cluster-info")

    system
  }

}
