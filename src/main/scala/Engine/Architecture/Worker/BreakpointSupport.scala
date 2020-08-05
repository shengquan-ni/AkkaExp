package Engine.Architecture.Worker

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.LocalBreakpoint

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

trait BreakpointSupport {
  var breakpoints = new Array[LocalBreakpoint](0)
  var unhandledFaultedTuples = new mutable.HashMap[Long,FaultedTuple]()

  def registerBreakpoint(breakpoint: LocalBreakpoint): Unit ={
    var i = 0
    Breaks.breakable {
      while (i < breakpoints.length) {
        if (breakpoints(i).id == breakpoint.id) {
          breakpoints(i) = breakpoint
          Breaks.break()
        }
        i += 1
      }
      breakpoints = breakpoints :+ breakpoint
    }
  }


  def removeBreakpoint(breakpointID:String): Unit ={
    val idx = breakpoints.indexWhere(_.id == breakpointID)
    if(idx != -1){
      val brk = breakpoints(idx)
      unhandledFaultedTuples(brk.triggeredTupleId) = new FaultedTuple(brk.triggeredTuple,brk.triggeredTupleId,brk.isInput)
      breakpoints = breakpoints.take(idx)
    }
  }

}
