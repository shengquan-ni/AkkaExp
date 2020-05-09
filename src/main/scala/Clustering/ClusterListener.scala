package Clustering

import Engine.Common.Constants
import akka.actor.{Actor, ActorLogging, Address, ExtendedActorSystem}
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
      if(context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress == member.address){
        if(Constants.masterNodeAddr != null){
          availableNodeAddresses.add(self.path.address)
          Engine.Common.Config.dataset += Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers += Engine.Common.Config.numWorkerPerNode
        }
      }else{
        if(Constants.masterNodeAddr != member.address.host.get){
          availableNodeAddresses.add(member.address)
          Engine.Common.Config.dataset += Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers += Engine.Common.Config.numWorkerPerNode
        }
      }
      println("---------Now we have "+availableNodeAddresses.size+" nodes in the cluster---------")
      println("dataset: "+Engine.Common.Config.dataset+" numWorkers: "+Constants.defaultNumWorkers)
    case UnreachableMember(member) =>
      if(context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress == member.address){
        if(Constants.masterNodeAddr != null) {
          availableNodeAddresses.remove(self.path.address)
          Engine.Common.Config.dataset -= Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers -= Engine.Common.Config.numWorkerPerNode
        }
      }else{
        if(Constants.masterNodeAddr != member.address.host.get) {
          availableNodeAddresses.remove(member.address)
          Engine.Common.Config.dataset -= Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers -= Engine.Common.Config.numWorkerPerNode
        }
      }
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      if(context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress == member.address){
        if(Constants.masterNodeAddr != null) {
          availableNodeAddresses.remove(self.path.address)
          Engine.Common.Config.dataset -= Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers -= Engine.Common.Config.numWorkerPerNode
        }
      }else{
        if(Constants.masterNodeAddr != member.address.host.get) {
          availableNodeAddresses.remove(member.address)
          Engine.Common.Config.dataset -= Engine.Common.Config.dataVolumePerNode
          Constants.defaultNumWorkers -= Engine.Common.Config.numWorkerPerNode
        }
      }
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case _: MemberEvent => // ignore
    case ClusterListener.GetAvailableNodeAddresses => sender ! availableNodeAddresses.toArray
  }


}
