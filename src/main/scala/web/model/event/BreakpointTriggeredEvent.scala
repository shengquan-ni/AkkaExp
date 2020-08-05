package web.model.event

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Controller.ControllerEvent
import akka.actor.ActorRef

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class BreakpointFault(actorRef:ActorRef, faultedTuple: FaultedTuple, messages: ArrayBuffer[String])

object BreakpointTriggeredEvent {
  def apply(event: ControllerEvent.BreakpointTriggered): BreakpointTriggeredEvent = {
    val faults = new mutable.MutableList[BreakpointFault]()
    for (elem <- event.report) {
      faults += BreakpointFault(elem._1._1, elem._1._2, elem._2)
    }
    BreakpointTriggeredEvent(faults, event.operatorID)
  }
}

case class BreakpointTriggeredEvent(
    report: mutable.MutableList[BreakpointFault],
    operatorID: String
) extends TexeraWsEvent
