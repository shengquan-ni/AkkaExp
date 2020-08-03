package web.model.request

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[HelloWorldRequest]),
    new Type(value = classOf[ExecuteWorkflowRequest]),
    new Type(value = classOf[PauseWorkflowRequest]),
    new Type(value = classOf[ResumeWorkflowRequest]),
    new Type(value = classOf[ModifyLogicRequest])
  )
)
trait TexeraWsRequest {}
