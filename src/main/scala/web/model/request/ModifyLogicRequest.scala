package web.model.request

import texera.common.workflow.{TexeraOperator}

import scala.beans.BeanProperty
import scala.collection.mutable

case class ModifyLogicRequest
(
  @BeanProperty workflowId: String,
  @BeanProperty newOperator: TexeraOperator
) extends TexeraWsRequest
