package Engine.Architecture.DeploySemantics.Layer

import Engine.Architecture.DeploySemantics.DeployStrategy.DeployStrategy
import Engine.Architecture.DeploySemantics.DeploymentFilter.DeploymentFilter
import Engine.Common.AmberTag.LayerTag
import Engine.Operators.OperatorMetadata
import akka.actor.{ActorContext, ActorRef, Address}

abstract class ActorLayer(val tag:LayerTag, var numWorkers:Int, val deploymentFilter: DeploymentFilter, val deployStrategy: DeployStrategy) extends Serializable {

  override def clone(): AnyRef = ???

  var layer:Array[ActorRef] = _

  def isBuilt: Boolean = layer != null

  def build(prev:Array[(OperatorMetadata,ActorLayer)], all:Array[Address])(implicit context:ActorContext): Unit

  override def hashCode(): Int = tag.hashCode()
}
