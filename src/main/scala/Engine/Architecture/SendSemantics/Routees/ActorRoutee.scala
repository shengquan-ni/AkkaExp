package Engine.Architecture.SendSemantics.Routees

import Engine.Common.AmberMessage.ControlMessage.{Pause, Resume}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import akka.actor.{Actor, ActorContext, ActorRef, PoisonPill, Props}

abstract class ActorRoutee(receiver: ActorRef) extends BaseRoutee(receiver) {

  var senderActor:ActorRef = _

  override def pause(): Unit = {
    senderActor ! Pause
  }

  override def resume(): Unit = {
    senderActor ! Resume
  }

  override def schedule(msg: DataMessage)(implicit sender: ActorRef): Unit = {
    senderActor ! msg
  }

  override def schedule(msg: EndSending)(implicit sender: ActorRef): Unit = {
    senderActor ! msg
  }

  override def dispose(): Unit = {
    senderActor ! PoisonPill
  }
}
