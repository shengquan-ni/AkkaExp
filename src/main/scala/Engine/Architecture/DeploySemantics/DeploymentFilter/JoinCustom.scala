package Engine.Architecture.DeploySemantics.DeploymentFilter

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OperatorMetadata
import akka.actor.Address

object JoinCustom{
  def apply(startIndex: Int, endIndex: Int) = new JoinCustom(startIndex, endIndex)
}


class JoinCustom (startIndex: Int, endIndex: Int) extends DeploymentFilter {
  override def filter(prev: Array[(OperatorMetadata, ActorLayer)], all: Array[Address], local: Address): Array[Address] = all.slice(startIndex, endIndex+1)
}