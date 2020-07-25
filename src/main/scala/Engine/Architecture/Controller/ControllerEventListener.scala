package Engine.Architecture.Controller

import Engine.Architecture.Controller.ControllerEvent.{WorkflowCompleted, WorkflowStatusUpdate}

case class ControllerEventListener
(
val workflowCompletedListener: WorkflowCompleted => Unit,
val workflowStatusUpdateListener: WorkflowStatusUpdate => Unit
)