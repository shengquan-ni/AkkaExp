package web.model.event

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes (Array(
  new Type(value = classOf[HelloWorldResponse]),
  new Type(value = classOf[WorkflowCompilationErrorEvent]),
  new Type(value = classOf[WorkflowStartedEvent]),
  new Type(value = classOf[WorkflowCompletedEvent]),
  new Type(value = classOf[WorkflowStatusUpdateEvent]),
  new Type(value = classOf[WorkflowPausedEvent]),
))
trait TexeraWsEvent {
}
