package Engine.Architecture.DeploySemantics.DeployStrategy

import akka.actor.Address

/**
 * It is used to specify how actors will be deployed on the cluster. eg: RoundRobin, Random. The next() function
 * gives the address of the machine to be used for the next actor.
 */
trait DeployStrategy extends Serializable {

  def initialize(available:Array[Address])

  def next():Address

}
