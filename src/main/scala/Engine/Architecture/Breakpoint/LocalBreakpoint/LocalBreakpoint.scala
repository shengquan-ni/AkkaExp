package Engine.Architecture.Breakpoint.LocalBreakpoint

import Engine.Common.AmberTuple.Tuple

abstract class LocalBreakpoint(val id:String,val version:Long) extends Serializable {

  def accept(tuple:Tuple)

  def isTriggered:Boolean

  var isReported = false

  var triggeredTuple:Tuple = _

  def isDirty:Boolean
}
