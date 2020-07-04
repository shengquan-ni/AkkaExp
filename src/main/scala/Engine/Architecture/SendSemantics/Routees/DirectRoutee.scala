package Engine.Architecture.SendSemantics.Routees

import java.text.SimpleDateFormat
import java.util.Date

import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.ControlMessage.RestartProcessing
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending, UpdateInputLinking}
import Engine.Common.AmberTag.LinkTag
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.ask

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext


class DirectRoutee(receiver:ActorRef) extends BaseRoutee(receiver) {
  val stash = new ArrayBuffer[Any]
  var isPaused = false
  val formatter = new SimpleDateFormat("HH:mm:ss.SSS z")
  var tag: LinkTag = null
  var senderActor: ActorRef = null

  var message: String = ""
  override def schedule(msg: DataMessage)(implicit sender: ActorRef): Unit = {
    if(isPaused){
      stash.append(msg)
    }else{
      receiver ! msg
    }
  }

  override def pause(): Unit = {
    isPaused = true
  }

  override def resume()(implicit sender: ActorRef): Unit = {
    isPaused = false
    for(i <- stash){
      i match{
        case d:DataMessage =>
          if(message.isEmpty) {
            message = d.payload(0).toString()
          }
          receiver ! d
        case e:EndSending => receiver ! e
      }
    }
    println(s"DIRECT ROUTEE for ${message} DONE with ${stash.size} at ${formatter.format(new Date(System.currentTimeMillis()))}")
    stash.clear()
  }

  override def schedule(msg: EndSending)(implicit sender: ActorRef): Unit = {
    if(isPaused){
      stash.append(msg)
    }else{
      receiver ! msg
    }
  }

  override def initialize(tag:LinkTag)(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    this.tag = tag
    this.senderActor = sender
    receiver ? UpdateInputLinking(sender,tag.from)
  }

  override def dispose(): Unit = {

  }

  override def toString: String = s"DirectRoutee($receiver)"

  override def propagateRestartForward(principalRef: ActorRef, mitigationCount:Int)(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    //println(s"SENDING RESTART TO ${receiver.toString()} from DIRECTROUTEE")
    AdvancedMessageSending.blockingAskWithRetry(receiver, RestartProcessing(principalRef, mitigationCount, senderActor, tag.from), 3)
  }
}
