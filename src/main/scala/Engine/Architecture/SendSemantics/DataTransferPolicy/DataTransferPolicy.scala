package Engine.Architecture.SendSemantics.DataTransferPolicy

import Engine.Architecture.SendSemantics.Routees.BaseRoutee
import Engine.Architecture.Worker.SkewMetricsFromPreviousWorker
import Engine.Common.AmberTag.LinkTag
import Engine.Common.AmberTuple.Tuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

/**
 * Policy between a worker actor and its downstream layer of actors. eg: HashBasedShufflePolicy, OneToOnePolicy etc.
 * @param batchSize
 */
abstract class DataTransferPolicy(var batchSize:Int) extends Serializable {
  var tag:LinkTag = _

  /**
   * It adds the tuple into the outgoing batch. When the batch is full, an outgoing message is scheduled
   * on the appropriate receiver (BaseRoutee.schedule())
   * @param tuple
   * @param sender
   */
  def accept(tuple:Tuple)(implicit sender: ActorRef = Actor.noSender):Unit

  def noMore()(implicit sender: ActorRef = Actor.noSender):Unit

  def pause():Unit

  def resume()(implicit sender:ActorRef):Unit

  def getFlowActors(): ArrayBuffer[ActorRef] = {
    return null
  }

  /**
   * Policy between a worker actor and its downstream layer of actors.
   * @param linkTag
   * @param next
   * @param ac
   * @param sender
   * @param timeout
   * @param ec
   * @param log
   */
  def initialize(linkTag:LinkTag, next:Array[BaseRoutee])(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Unit = {
    this.tag = linkTag
    //next.foreach(x => log.info("link: {}",x))
  }

  def dispose():Unit

}
