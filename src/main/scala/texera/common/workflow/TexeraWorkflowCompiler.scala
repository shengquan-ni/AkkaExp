package texera.common.workflow

import Engine.Architecture.Controller.Workflow
import Engine.Common.AmberTag.OperatorTag
import Engine.Operators.OperatorMetadata
import texera.common.{TexeraConstraintViolation, TexeraContext}

import scala.collection.mutable

class TexeraWorkflowCompiler(texeraWorkflow: TexeraWorkflow, context: TexeraContext) {

  def init(): Unit = {
    this.texeraWorkflow.operators.foreach(o => o.context = context)
  }

  def validate: Map[String, Set[TexeraConstraintViolation]] =
    this.texeraWorkflow.operators.map(o => o.operatorID -> o.validate).toMap.filter(pair => pair._2.nonEmpty)

  def amberWorkflow: Workflow = {
    val amberOperators: mutable.Map[OperatorTag, OperatorMetadata] = mutable.Map()
    texeraWorkflow.operators.foreach(o => {
      val amberOperator = o.amberOperator
      amberOperators.put(amberOperator.tag, amberOperator)
    })

    val outLinks: mutable.Map[OperatorTag, mutable.Set[OperatorTag]] = mutable.Map()
    texeraWorkflow.links.foreach(link => {
      val origin = OperatorTag(this.context.workflowID, link.origin)
      val dest = OperatorTag(this.context.workflowID, link.destination)
      val destSet = outLinks.getOrElse(origin, mutable.Set())
      destSet.add(dest)
      outLinks.update(origin, destSet)
    })

    val outLinksImmutableValue: mutable.Map[OperatorTag, Set[OperatorTag]] = mutable.Map()
    outLinks.foreach(entry => {
      outLinksImmutableValue.update(entry._1, entry._2.toSet)
    })
    val outLinksImmutable: Map[OperatorTag, Set[OperatorTag]] = outLinksImmutableValue.toMap

    new Workflow(amberOperators, outLinksImmutable)
  }
}
