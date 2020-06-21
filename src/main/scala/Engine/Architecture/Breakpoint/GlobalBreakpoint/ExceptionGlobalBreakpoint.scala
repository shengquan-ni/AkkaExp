package Engine.Architecture.Breakpoint.GlobalBreakpoint

import Engine.Architecture.Breakpoint.LocalBreakpoint.{ConditionalBreakpoint, ExceptionBreakpoint, LocalBreakpoint}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.AssignBreakpoint
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class ExceptionGlobalBreakpoint(id:String) extends GlobalBreakpoint(id) {
  var exceptions: ArrayBuffer[ExceptionBreakpoint] = new ArrayBuffer[ExceptionBreakpoint]()

  override def acceptImpl(sender: ActorRef, localBreakpoint: LocalBreakpoint): Unit = {
    if (localBreakpoint.isTriggered) {
      exceptions.append(localBreakpoint.asInstanceOf[ExceptionBreakpoint])
    }
  }

  override def isTriggered: Boolean = exceptions.nonEmpty

  override def partitionImpl(layer: Array[ActorRef])(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter, id: String, version: Long): Iterable[ActorRef] = {
    for (x <- layer) {
    AdvancedMessageSending.blockingAskWithRetry(x, AssignBreakpoint(new ExceptionBreakpoint()), 10)
  }
  layer
}

  override def report(): String = {
    var result = s"Exception Breakpoint[$id]: \n"
    for(i <- exceptions){
      result ++= "["+(if(i.isInput)"IN" else "OUT")+"]"+i.triggeredTuple.toString+" triggered the breakpoint \n ERROR: "+i.error.toString+" \n"
    }
    exceptions.clear()
    result
  }

  override def isCompleted: Boolean = false
}
