package Engine.Architecture.Breakpoint.LocalBreakpoint

import Engine.Common.AmberTuple.Tuple

class CountBreakpoint(val target:Long)(implicit id:String,version:Long) extends LocalBreakpoint(id,version){

  var current:Long = 0

  override def accept(tuple: Tuple): Unit = {
    current += 1
  }

  override def needUserFix: Boolean = false

  override def isTriggered: Boolean = current == target

  override def isDirty: Boolean = isReported
}
