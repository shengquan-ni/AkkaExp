package Engine.Architecture.Controller

import Engine.Architecture.Principal.{PrincipalState, PrincipalStatistics}
import Engine.Common.AmberTuple.Tuple

object ControllerEvent {

  case class WorkflowCompleted
  (
  // map from sink operator ID to the result list of tuples
  result: Map[String, List[Tuple]]
  )

  case class WorkflowStatusUpdate
  (
  operatorStatistics: Map[String, PrincipalStatistics]
  )

  case class ModifyLogicCompleted()

  case class BreakpointTriggered()

}
