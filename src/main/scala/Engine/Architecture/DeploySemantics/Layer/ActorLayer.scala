package Engine.Architecture.DeploySemantics.Layer

import Engine.Architecture.DeploySemantics.DeployStrategy.DeployStrategy
import Engine.Architecture.DeploySemantics.DeploymentFilter.DeploymentFilter
import Engine.Common.AmberTag.LayerTag
import Engine.Operators.OperatorMetadata
import akka.actor.{ActorContext, ActorRef, Address}

/**
 * There are two implementations of ActorLayers - ProcessorWorkerLayer and GeneratorWorkerLayer
 * @param tag
 * @param numWorkers
 * @param deploymentFilter
 * @param deployStrategy
 */
abstract class ActorLayer(val tag:LayerTag, var numWorkers:Int, val deploymentFilter: DeploymentFilter, val deployStrategy: DeployStrategy) extends Serializable {

  override def clone(): AnyRef = ???

  var layer:Array[ActorRef] = _

  def isBuilt: Boolean = layer != null

  /**
   * building a layer means creating the actual actors and placing them on the appropriate machines.
   * The created actors are then put in the ''layer'' array.
   */
  def build(prev:Array[(OperatorMetadata,ActorLayer)], all:Array[Address])(implicit context:ActorContext): Unit

  override def hashCode(): Int = tag.hashCode()
}
