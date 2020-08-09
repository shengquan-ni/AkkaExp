package Engine.Architecture.Controller

import Engine.Architecture.Controller.ControllerEvent.{BreakpointTriggered, ModifyLogicCompleted, SkipTupleResponse, WorkflowCompleted, WorkflowPaused, WorkflowStatusUpdate}

case class ControllerEventListener(
    workflowCompletedListener: WorkflowCompleted => Unit = null,
    workflowStatusUpdateListener: WorkflowStatusUpdate => Unit = null,
    modifyLogicCompletedListener: ModifyLogicCompleted => Unit = null,
    breakpointTriggeredListener: BreakpointTriggered => Unit = null,
    workflowPausedListener: WorkflowPaused => Unit = null,
    skipTupleResponseListener: SkipTupleResponse => Unit = null,
)
