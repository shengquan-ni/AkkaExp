package texera.common.workflow

import java.util.UUID

import Engine.Common.AmberTag.OperatorTag
import Engine.Operators.OperatorMetadata
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import texera.common.schema.{PropertyNameConstants, TexeraOperatorDescription}
import texera.common.{TexeraConstraintViolation, TexeraContext}
import texera.operators.scan.TexeraLocalFileScan
import texera.operators.sink.TexeraAdhocSink
import texera.operators.sleep.TexeraSleepOperator

import scala.collection.{JavaConverters, mutable}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(Array(
  new Type(value = classOf[TexeraLocalFileScan], name = "LocalFileScan"),
  new Type(value = classOf[TexeraAdhocSink], name = "AdhocSink"),
  new Type(value = classOf[TexeraSleepOperator], name = "Sleep")
))
abstract class TexeraOperator {

  @JsonIgnore var context: TexeraContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID) var operatorID: String = UUID.randomUUID.toString

  def amberOperatorTag: OperatorTag = OperatorTag.apply(this.context.workflowID, this.operatorID)

  def amberOperator: OperatorMetadata

  def texeraOperatorDescription: TexeraOperatorDescription

  def validate(): Set[TexeraConstraintViolation] = {
    if (this.context.validator == null) {
      Set()
    } else {
      TexeraConstraintViolation.of(JavaConverters.asScalaSet(this.context.validator.validate(this))).toSet
    }
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)


}
