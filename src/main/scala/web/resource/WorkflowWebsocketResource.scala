package web.resource

import java.util
import java.util.concurrent.atomic.AtomicInteger

import Engine.Architecture.Controller.ControllerEvent.WorkflowStatusUpdate
import Engine.Architecture.Controller.{Controller, ControllerEventListener}
import Engine.Common.AmberMessage.ControlMessage.{ModifyLogic, Start}
import Engine.Common.AmberMessage.ControllerMessage.AckedControllerInitialization
import Engine.Common.AmberTag.WorkflowTag
import akka.actor.ActorRef
import javax.websocket
import javax.websocket.{CloseReason, OnClose, OnMessage, OnOpen, Session}
import javax.websocket.server.ServerEndpoint
import texera.common.{TexeraContext, TexeraUtils}
import texera.common.schema.OperatorSchemaGenerator
import texera.common.workflow.{TexeraWorkflow, TexeraWorkflowCompiler}
import web.TexeraWebApplication
import web.model.event.{HelloWorldResponse, ModifyLogicCompletedEvent, TexeraWsEvent, WorkflowCompilationErrorEvent, WorkflowCompletedEvent, WorkflowStatusUpdateEvent}
import web.model.request.{ExecuteWorkflowRequest, HelloWorldRequest, ModifyLogicRequest, PauseWorkflowRequest, TexeraWsRequest}

import scala.collection.mutable

object WorkflowWebsocketResource {

  val nextWorkflowID = new AtomicInteger(0)

  val sessionMap = new mutable.HashMap[String, Session]
  val sessionJobs = new mutable.HashMap[String, ActorRef]

}

@ServerEndpoint("/wsapi/workflow-websocket")
class WorkflowWebsocketResource {

  final val objectMapper = TexeraUtils.objectMapper

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    WorkflowWebsocketResource.sessionMap.update(session.getId, session)
    println("connection open")
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    println(message)
    val request = objectMapper.readValue(message, classOf[TexeraWsRequest])
    println(request)
    request match {
      case helloWorld: HelloWorldRequest =>
        send(session, HelloWorldResponse("hello from texera web server"))
      case execute: ExecuteWorkflowRequest =>
        println(execute)
        executeWorkflow(session, execute)
      case newLogic: ModifyLogicRequest =>
        println(newLogic)
        modifyLogic(session, newLogic)

    }

  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
  }

  def send(session: Session, event: TexeraWsEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(event))
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
    val controller: ActorRef = WorkflowWebsocketResource.sessionJobs(session.getId)
    controller ! ModifyLogic(newLogic.newOperatorMetadata)
  }

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    val ctx = new TexeraContext
    val workflowID = Integer.toString(WorkflowWebsocketResource.nextWorkflowID.incrementAndGet)
    ctx.workflowID = workflowID
    val texeraWorkflowCompiler = new TexeraWorkflowCompiler(TexeraWorkflow(request.operators, request.links), ctx)

    texeraWorkflowCompiler.init()
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowCompilationErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowTag.apply(workflowID)

    val eventListener = ControllerEventListener(
      completed => {
        send(session, WorkflowCompletedEvent.apply(completed))
        WorkflowWebsocketResource.sessionJobs.remove(session.getId)
      },
      statusUpdate => {
        println(statusUpdate.operatorStatistics)
        send(session, WorkflowStatusUpdateEvent(statusUpdate.operatorStatistics))
      },
      modifyLogicCompleted => {
        send(session, ModifyLogicCompletedEvent())
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflowTag, workflow, false, eventListener, 100))
    controllerActorRef! AckedControllerInitialization
    controllerActorRef! Start

    WorkflowWebsocketResource.sessionJobs(session.getId) = controllerActorRef
  }


}
