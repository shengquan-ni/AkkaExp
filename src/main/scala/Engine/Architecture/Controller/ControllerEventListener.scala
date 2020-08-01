package Engine.Architecture.Controller

import Engine.Architecture.Controller.ControllerEvent.{WorkflowCompleted, WorkflowStatusUpdate, ModifyLogicCompleted, BreakpointTriggered}

case class ControllerEventListener
(
  val workflowCompletedListener: WorkflowCompleted => Unit,
  val workflowStatusUpdateListener: WorkflowStatusUpdate => Unit,
  val modifyLogicCompletedListener: ModifyLogicCompleted => Unit,
  val breakpointTriggeredListener: BreakpointTriggered => Unit,
)