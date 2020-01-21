package Engine.Common.AmberMessage

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Controller.ControllerState
import Engine.Common.AmberTag.OperatorTag


object ControllerMessage{
  final case class AckedControllerInitialization()

  final case class ContinuedInitialization()

  final case class ReportState(controllerState:ControllerState.Value)

  final case class ReportGlobalBreakpointTriggered(report:String)

  final case class PassBreakpointTo(operatorID:String, breakpoint:GlobalBreakpoint)
}





