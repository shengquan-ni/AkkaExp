package web.model.event

import texera.common.TexeraConstraintViolation

import scala.beans.BeanProperty

case class WorkflowCompilationErrorEvent
(
  @BeanProperty violations: Map[String, Set[TexeraConstraintViolation]]
) extends TexeraWsEvent
