package texera.common.workflow

import scala.beans.BeanProperty
import scala.collection.mutable

case class TexeraWorkflow
(
  @BeanProperty operators: mutable.MutableList[TexeraOperator],
  @BeanProperty links: mutable.MutableList[TexeraOperatorLink]
)
