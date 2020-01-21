package Clustering

import akka.actor.{Actor, ActorLogging, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberRemoved, MemberUp, UnreachableMember}

import scala.collection.mutable

object ClusterListener{
  final case class GetAvailableNodeAddresses()
}


class ClusterListener extends Actor with ActorLogging  {

  val cluster = Cluster(context.system)
  val availableNodeAddresses = new mutable.HashSet[Address]()

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case MemberUp(member) =>
      availableNodeAddresses.add(member.address)
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      availableNodeAddresses.remove(member.address)
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      availableNodeAddresses.remove(member.address)
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case _: MemberEvent => // ignore
    case ClusterListener.GetAvailableNodeAddresses => sender ! availableNodeAddresses.toArray
  }


}
