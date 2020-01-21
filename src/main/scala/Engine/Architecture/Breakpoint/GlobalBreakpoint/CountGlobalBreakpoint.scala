package Engine.Architecture.Breakpoint.GlobalBreakpoint
import Engine.Architecture.Breakpoint.LocalBreakpoint.{CountBreakpoint, LocalBreakpoint}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{AssignBreakpoint, QueryBreakpoint, QueryTriggeredBreakpoints, RemoveBreakpoint}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class CountGlobalBreakpoint(id:String, val target:Long) extends GlobalBreakpoint(id) {

  var current:Long = 0

  override def acceptImpl(sender:ActorRef, localBreakpoint: LocalBreakpoint): Unit = {
    current += localBreakpoint.asInstanceOf[CountBreakpoint].current
  }

  override def isTriggered: Boolean = current == target

  override def partitionImpl(layer: Array[ActorRef])(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter, id:String, version:Long): Iterable[ActorRef] = {
    val remaining = target - current
    var currentSum = 0L
    val length = layer.length
    var i = 0
    if(remaining/length > 0) {
      while(i < length - 1){
        AdvancedMessageSending.blockingAskWithRetry(layer(i),AssignBreakpoint(new CountBreakpoint(remaining/length)),10)
        currentSum += remaining/length
        i += 1
      }
      AdvancedMessageSending.blockingAskWithRetry(layer.last, AssignBreakpoint(new CountBreakpoint(remaining-currentSum)),10)
      layer
    }else{
      AdvancedMessageSending.blockingAskWithRetry(layer.last, AssignBreakpoint(new CountBreakpoint(remaining)),10)
      Array(layer.last)
    }
  }

  override def isRepartitionRequired: Boolean = unReportedWorkers.isEmpty && target != current

  override def report(): String = s"Count Breakpoint[$id]: current = $current, target = $target"

  override def isCompleted: Boolean = isTriggered

}
