package web.model.event

import Engine.Architecture.Controller.ControllerEvent.WorkflowCompleted

import scala.collection.mutable

case class OperatorResult(operatorID: String, table: List[Map[String, Any]])

object WorkflowCompletedEvent {

  // transform results in amber tuple format to the format accepted by frontend
  def apply(workflowCompleted: WorkflowCompleted): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
      val operatorID = pair._1
      val table = new mutable.MutableList[Map[String, Any]]
      for (tuple <- pair._2) {
        val tupleMap = new mutable.HashMap[String, Any]
        for (column <- Range.apply(0, tuple.length)) {
          val columnName = "_c" + column.toString
          tupleMap(columnName) = tuple.get(column)
        }
        table += tupleMap.toMap
      }
      resultList += OperatorResult(operatorID, table.toList)
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWsEvent
