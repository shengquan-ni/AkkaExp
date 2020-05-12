package Engine.Architecture.SendSemantics.Routees

import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.UpdateInputLinking
import Engine.Common.AmberTag.LinkTag
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class FlowControlRoutee(receiver:ActorRef) extends ActorRoutee(receiver) {
  /**
   * It sends a message to receiver to update its upstream links record.
   * @param tag
   * @param ac
   * @param sender
   * @param timeout
   * @param ec
   * @param log
   */
  override def initialize(tag:LinkTag)(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    senderActor = ac.actorOf(FlowControlSenderActor.props(receiver))
    AdvancedMessageSending.blockingAskWithRetry(receiver,UpdateInputLinking(senderActor,tag.from),10)
  }

  override def toString: String = s"FlowControlRoutee($receiver)"
}
