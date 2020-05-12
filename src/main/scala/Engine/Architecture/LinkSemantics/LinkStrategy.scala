package Engine.Architecture.LinkSemantics

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Common.AmberTag.LinkTag
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

/**
 * There are many LinkStrategies like HashBasedShuffle, LocalOneToOne, LocalRoundRobin. LinkStrategy is between two layers of workers.
 * LinkStrategy has a ''link'' function in which ''DataTransferPolicy'' is updated for each actor in the from layer.
 * @param from
 * @param to
 * @param batchSize
 */
abstract class LinkStrategy(val from:ActorLayer, val to:ActorLayer, val batchSize:Int) extends Serializable {

  val tag = LinkTag(from.tag,to.tag)

  /**
   * Iterates over all actors in the ''from'' layer and updates its DataTransferPolicy by sending a message to the worker actor.
   * @param timeout
   * @param ec
   * @param log
   */
  def link()(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit
}
