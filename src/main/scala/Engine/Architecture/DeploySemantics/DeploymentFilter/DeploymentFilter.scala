package Engine.Architecture.DeploySemantics.DeploymentFilter

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OperatorMetadata
import akka.actor.Address

/**
 * It filters out the machines that shouldn't be used by DeploymentStrategy.
 */
trait DeploymentFilter extends Serializable {

  def filter(prev: Array[(OperatorMetadata, ActorLayer)], all: Array[Address], local: Address):Array[Address]



}
