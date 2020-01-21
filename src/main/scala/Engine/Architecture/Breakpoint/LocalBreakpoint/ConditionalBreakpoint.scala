package Engine.Architecture.Breakpoint.LocalBreakpoint

import Engine.Common.AmberTuple.Tuple

class ConditionalBreakpoint(val predicate:Tuple => Boolean)(implicit id:String, version:Long) extends LocalBreakpoint(id,version) {

  var badTuple:Tuple = _

  override def accept(tuple: Tuple): Unit = {
    if(predicate(tuple)){
      badTuple = tuple
    }
  }

  override def isTriggered: Boolean = badTuple != null

  override def isFaultedTuple: Boolean = isTriggered

  override def isDirty: Boolean = isTriggered
}
