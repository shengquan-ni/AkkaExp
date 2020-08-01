package web.model.request

import Engine.Operators.OperatorMetadata
import texera.common.workflow.TexeraOperator

import scala.beans.BeanProperty
import scala.collection.mutable

case class ModifyLogicRequest
(
  newOperatorMetadata: OperatorMetadata
) extends TexeraWsRequest
