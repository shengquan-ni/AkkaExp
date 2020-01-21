package Engine.Architecture.Worker

import Engine.Architecture.Breakpoint.LocalBreakpoint.LocalBreakpoint

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

trait BreakpointSupport {
  var breakpoints = new Array[LocalBreakpoint](0)

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
      breakpoints = breakpoints.take(idx)
    }
  }

}
