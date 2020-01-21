package Engine.Architecture.SendSemantics.Routees

import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending, UpdateInputLinking}
import Engine.Common.AmberTag.LinkTag
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.ExecutionContext


class DirectRoutee(receiver:ActorRef) extends BaseRoutee(receiver) {
  override def schedule(msg: DataMessage)(implicit sender: ActorRef): Unit = {
    receiver ! msg
  }

  override def pause(): Unit = {

  }

  override def resume(): Unit = {

  }

  override def schedule(msg: EndSending)(implicit sender: ActorRef): Unit = {
    receiver ! msg
  }

  override def initialize(tag:LinkTag)(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    receiver ? UpdateInputLinking(sender,tag.from)
  }

  override def dispose(): Unit = {

  }

  override def toString: String = s"DirectRoutee($receiver)"
}
