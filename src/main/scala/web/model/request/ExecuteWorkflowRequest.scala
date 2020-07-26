package web.model.request

import texera.common.workflow.{TexeraOperator, TexeraOperatorLink}

import scala.beans.BeanProperty
import scala.collection.mutable

case class ExecuteWorkflowRequest
(
  @BeanProperty operators: mutable.MutableList[TexeraOperator],
  @BeanProperty links: mutable.MutableList[TexeraOperatorLink]
) extends TexeraWsRequest
