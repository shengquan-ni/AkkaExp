package Engine.Architecture.DeploySemantics.DeploymentFilter
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OperatorMetadata
import akka.actor.Address

object FollowPrevious{
  def apply() = new FollowPrevious()
}



class FollowPrevious extends DeploymentFilter {
  override def filter(prev: Array[(OperatorMetadata, ActorLayer)], all: Array[Address], local: Address): Array[Address] ={
    val result = prev.flatMap(x => x._2.layer.map(y => y.path.address)).distinct.intersect(all)
    println(result)
    if(result.isEmpty) all else result //fall back to UseAll when there is nothing to follow
  }
}
