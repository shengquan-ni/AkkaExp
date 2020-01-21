package Engine.Common


object ThreadState extends Enumeration {
  val Idle,
  Running,
  Paused,
  LocalBreakpointTriggered,
  Completed = Value
}
