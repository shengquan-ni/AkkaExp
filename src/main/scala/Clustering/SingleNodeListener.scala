package Clustering

import Clustering.ClusterListener.GetAvailableNodeAddresses
import akka.actor.{Actor, ActorLogging}

class SingleNodeListener extends Actor with ActorLogging {
  override def receive: Receive = {
    case GetAvailableNodeAddresses => sender ! Array(context.self.path.address)
  }
}
