package texera.common

import javax.validation.ConstraintViolation
import texera.common.workflow.TexeraOperator

import scala.beans.BeanProperty
import scala.collection.mutable

object TexeraConstraintViolation
{
  def apply(violation: ConstraintViolation[TexeraOperator]): TexeraConstraintViolation =
    new TexeraConstraintViolation(violation.getMessage, violation.getPropertyPath.toString)

  def of(violations: mutable.Set[ConstraintViolation[TexeraOperator]]): mutable.Set[TexeraConstraintViolation] =
    violations.map(v => TexeraConstraintViolation(v))
}

case class TexeraConstraintViolation
(
@BeanProperty message: String,
@BeanProperty propertyPath: String
)
