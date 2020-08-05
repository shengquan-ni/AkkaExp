package Engine.Architecture.Controller

import Engine.Architecture.Controller.ControllerEvent.{BreakpointTriggered, ModifyLogicCompleted, SkipTupleResponse, WorkflowCompleted, WorkflowPaused, WorkflowStatusUpdate}

case class ControllerEventListener(
    val workflowCompletedListener: WorkflowCompleted => Unit,
    val workflowStatusUpdateListener: WorkflowStatusUpdate => Unit,
    val modifyLogicCompletedListener: ModifyLogicCompleted => Unit,
    val breakpointTriggeredListener: BreakpointTriggered => Unit,
    val workflowPausedListener: WorkflowPaused => Unit,
    val skipTupleResponseListener: SkipTupleResponse => Unit,
)
