package web.resource

import java.util.concurrent.atomic.AtomicInteger

import Engine.Architecture.Controller.{Controller, ControllerEventListener}
import Engine.Common.AmberMessage.ControlMessage.{ModifyLogic, Pause, Resume, SkipTuple, SkipTupleGivenWorkerRef, Start}
import Engine.Common.AmberMessage.ControllerMessage.AckedControllerInitialization
import Engine.Common.AmberTag.WorkflowTag
import akka.actor.ActorRef
import javax.websocket.server.ServerEndpoint
import javax.websocket._
import texera.common.workflow.{TexeraWorkflow, TexeraWorkflowCompiler}
import texera.common.{TexeraContext, TexeraUtils}
import web.TexeraWebApplication
import web.model.event._
import web.model.request._

import scala.collection.mutable

object WorkflowWebsocketResource {

  val nextWorkflowID = new AtomicInteger(0)

  val sessionMap = new mutable.HashMap[String, Session]
  val sessionJobs = new mutable.HashMap[String, (TexeraWorkflowCompiler, ActorRef)]

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
      case pause: PauseWorkflowRequest =>
        pauseWorkflow(session)
      case resume: ResumeWorkflowRequest =>
        resumeWorkflow(session)
      case skipTupleMsg: SkipTupleRequest =>
        skipTuple(session, skipTupleMsg)
    }

  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {}

  def send(session: Session, event: TexeraWsEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(event))
  }

  def skipTuple(session: Session, tupleReq: SkipTupleRequest): Unit = {
    val actorRef = tupleReq.actorRef
    val faultedTuple = tupleReq.faultedTuple
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! SkipTupleGivenWorkerRef(actorRef,faultedTuple)
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
    val texeraOperator = newLogic.operator
    val (compiler, controller) = WorkflowWebsocketResource.sessionJobs(session.getId)
    compiler.initOperator(texeraOperator)
    controller ! ModifyLogic(texeraOperator.amberOperator)
  }

  def pauseWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Pause
    // workflow paused event will be send after workflow is actually paused
    // the callback function will handle sending the paused event to frontend
  }

  def resumeWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Resume
    send(session, WorkflowResumedEvent())
  }

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    val context = new TexeraContext
    val workflowID = Integer.toString(WorkflowWebsocketResource.nextWorkflowID.incrementAndGet)
    context.workflowID = workflowID
    val texeraWorkflowCompiler = new TexeraWorkflowCompiler(
      TexeraWorkflow(request.operators, request.links, request.breakpoints),
      context
    )

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
        send(session, WorkflowStatusUpdateEvent(statusUpdate.operatorStatistics))
      },
      modifyLogicCompleted => {
        send(session, ModifyLogicCompletedEvent())
      },
      breakpointTriggered => {
        send(session, BreakpointTriggeredEvent.apply(breakpointTriggered))
      },
      workflowPaused => {
        send(session, WorkflowPausedEvent())
      },
      skipTupleResponse => {
        send(session, SkipTupleResponseEvent())
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflowTag, workflow, false, eventListener, 100)
    )
    controllerActorRef ! AckedControllerInitialization
    texeraWorkflowCompiler.initializeBreakpoint(controllerActorRef)
    controllerActorRef ! Start

    WorkflowWebsocketResource.sessionJobs(session.getId) =
      (texeraWorkflowCompiler, controllerActorRef)

    send(session, WorkflowStartedEvent())

  }

}
