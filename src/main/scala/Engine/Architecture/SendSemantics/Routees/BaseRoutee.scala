package Engine.Architecture.SendSemantics.Routees

import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.LinkTag
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

/**
 * BaseRoutee is the class about how the receivers will receive the data being sent. It has two subclasses -
 * ''DirectRoutee'' where the actor directly sends data to the receiver; and ''ActorRoutee'' where the data is sent to
 * an actor which then sends it to the receiver.
 * @param receiver
 */
abstract class BaseRoutee(val receiver:ActorRef) extends Serializable {

  def initialize(tag:LinkTag)(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter)

  def schedule(msg:DataMessage)(implicit sender: ActorRef)

  def pause()

  def resume()(implicit sender: ActorRef)

  def schedule(msg:EndSending)(implicit sender: ActorRef)

  def dispose()

  def getSenderActor(): ActorRef = {
    return null
  }

  def propagateRestartForward()(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {

  }
}
