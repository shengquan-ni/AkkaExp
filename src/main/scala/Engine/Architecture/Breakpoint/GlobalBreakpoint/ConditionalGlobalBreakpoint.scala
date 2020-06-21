package Engine.Architecture.Breakpoint.GlobalBreakpoint

import Engine.Architecture.Breakpoint.LocalBreakpoint.{ConditionalBreakpoint, CountBreakpoint, LocalBreakpoint}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{AssignBreakpoint, QueryBreakpoint, RemoveBreakpoint}
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class ConditionalGlobalBreakpoint(id:String, val predicate:Tuple =>Boolean) extends GlobalBreakpoint(id) {

  var badTuples:ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()

  override def acceptImpl(sender:ActorRef, localBreakpoint: LocalBreakpoint):Unit = {
    if(localBreakpoint.isTriggered){
      badTuples.append(localBreakpoint.asInstanceOf[ConditionalBreakpoint].triggeredTuple)
    }
  }

  override def isTriggered: Boolean = badTuples.nonEmpty

  override def partitionImpl(layer: Array[ActorRef])(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter, id:String, version:Long): Iterable[ActorRef] = {
    for(x <- layer) {
      AdvancedMessageSending.blockingAskWithRetry(x,AssignBreakpoint(new ConditionalBreakpoint(predicate)),10)
    }
    layer
  }


  override def report(): String = {
    val result = s"Conditional Breakpoint[$id]: ${badTuples.mkString(",")} triggered the breakpoint"
    badTuples.clear()
    result
  }

  override def isCompleted: Boolean = false

}
