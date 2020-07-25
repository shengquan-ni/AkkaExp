package web.model.event

import Engine.Architecture.Controller.ControllerEvent.WorkflowCompleted
import Engine.Common.AmberTuple.Tuple

case class WorkflowCompletedEvent(result: Map[String, List[Tuple]]) extends TexeraWsEvent
