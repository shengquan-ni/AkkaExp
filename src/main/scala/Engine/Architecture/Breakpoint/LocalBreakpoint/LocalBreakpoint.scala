package Engine.Architecture.Breakpoint.LocalBreakpoint

import Engine.Common.AmberTuple.Tuple

abstract class LocalBreakpoint(val id:String,val version:Long) extends Serializable {

  def accept(tuple:Tuple)

  def isTriggered:Boolean

  def isFaultedTuple:Boolean

  var isReported = false

  def isDirty:Boolean
}
