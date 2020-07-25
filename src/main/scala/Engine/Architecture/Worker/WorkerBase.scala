package Engine.Architecture.Worker


import Engine.Common.AmberException.AmberException
import Engine.Common.AmberMessage.ControlMessage.{Ack, CollectSinkResults, LocalBreakpointTriggered, Pause, QueryState, QueryStatistics, ReleaseOutput, RequireAck, Resume, Start, StashOutput}
import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, AssignBreakpoint, DataMessage, EndSending, ExecutionCompleted, ExecutionPaused, QueryBreakpoint, QueryTriggeredBreakpoints, RemoveBreakpoint, ReportFailure, ReportState, ReportStatistics, ReportedQueriedBreakpoint, ReportedTriggeredBreakpoints, UpdateOutputLinking}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.ElidableStatement
import akka.actor.{Actor, ActorLogging, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks
import scala.concurrent.duration._

abstract class WorkerBase extends Actor with ActorLogging with Stash with DataTransferSupport {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout:Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  var pausedFlag = false
  @elidable(INFO) var startTime = 0L

  def onInitialization(): Unit = {

  }

  def onStart(): Unit ={
    startTime = System.nanoTime()
    context.parent ! ReportState(WorkerState.Running)
  }

  def onPausing():Unit={
    pausedFlag = true
    pauseDataTransfer()
  }

  def onPaused(): Unit ={
    context.parent ! ReportState(WorkerState.Paused)
  }

  def onResuming():Unit = {
    resumeDataTransfer()
    pausedFlag = false
  }

  def onResumed(): Unit ={
    context.parent ! ReportState(WorkerState.Running)
  }

  def onCompleting():Unit = {
    endDataTransfer()
  }

  def onCompleted(): Unit ={
    context.parent ! ReportState(WorkerState.Completed)
  }

  def onInterrupted(operations: => Unit): Unit ={
    if(pausedFlag){
      pauseDataTransfer()
      operations
      Breaks.break()
    }
  }

  def onBreakpointTriggered(): Unit = {
    context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
  }

  def getOutputRowCount(): Int

  def getResultTuples(): mutable.MutableList[Tuple] = {
    mutable.MutableList()
  }

  final def allowStashOrReleaseOutput:Receive = {
    case StashOutput =>
      sender ! Ack
      pauseDataTransfer()
    case ReleaseOutput =>
      sender ! Ack
      resumeDataTransfer()
  }

  final def allowModifyBreakpoints:Receive = {
    case AssignBreakpoint(bp) =>
      log.info("Assign breakpoint: "+bp.id)
      registerBreakpoint(bp)
      sender ! Ack
    case RemoveBreakpoint(id) =>
      log.info("Remove breakpoint: "+id)
      sender ! Ack
      removeBreakpoint(id)
  }

  final def disallowModifyBreakpoints:Receive = {
    case AssignBreakpoint(bp) =>
      sender ! Ack
      throw new AmberException(s"Assignation of breakpoint ${bp.id} is not allowed at this time")
    case RemoveBreakpoint(id) =>
      sender ! Ack
      throw new AmberException(s"Removal of breakpoint $id is not allowed at this time")
  }

  final def allowQueryBreakpoint:Receive = {
    case QueryBreakpoint(id) =>
      val toReport = breakpoints.find(_.id == id)
      if(toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
      }else{
        throw new AmberException(s"breakpoint $id not found when query")
      }
  }

  final def disallowQueryBreakpoint:Receive = {
    case QueryBreakpoint(id) =>
      throw new AmberException(s"query breakpoint $id is not allowed at this time")
  }

  final def allowQueryTriggeredBreakpoints:Receive = {
    case QueryTriggeredBreakpoints =>
      val toReport = breakpoints.filter(_.isTriggered)
      if(toReport.nonEmpty){
        toReport.foreach(_.isReported = true)
        sender ! ReportedTriggeredBreakpoints(toReport)
      }else{
        throw new AmberException("no triggered local breakpoints but worker in triggered breakpoint state")
      }
  }

  final def disallowQueryTriggeredBreakpoints:Receive = {
    case QueryTriggeredBreakpoints =>
      throw new AmberException(s"query triggered breakpoints is not allowed at this time")
  }

  final def allowUpdateOutputLinking:Receive = {
    case UpdateOutputLinking(policy, tag, receivers)=>
      sender ! Ack
      updateOutput(policy, tag, receivers)
  }

  final def disallowUpdateOutputLinking:Receive = {
    case UpdateOutputLinking(policy, tag, receivers)=>
      sender ! Ack
      throw new AmberException(s"update output link information of $tag is not allowed at this time")
  }

  final def stashOthers:Receive = {
    case msg =>
      log.info("stashing: "+msg)
      stash()
  }

  final def discardOthers:Receive = {
    case msg => log.info(s"discarding: $msg")
  }

  override def receive:Receive = {
    case AckedWorkerInitialization =>
      onInitialization()
      context.parent ! ReportState(WorkerState.Ready)
      context.become(ready)
      unstashAll()
    case QueryState =>
      sender ! ReportState(WorkerState.Uninitialized)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Uninitialized, getOutputRowCount()))
    case _ => stash()
  }

  def ready:Receive =
    allowStashOrReleaseOutput orElse
    allowUpdateOutputLinking orElse //update linking
    allowModifyBreakpoints orElse //modify break points
    disallowQueryBreakpoint orElse  //query specific breakpoint
    disallowQueryTriggeredBreakpoints orElse[Any, Unit] { //query triggered breakpoint
    case Start =>
      sender ! Ack
      onStart()
    case Pause =>
      onPaused()
      context.become(pausedBeforeStart)
      unstashAll()
    case Resume => context.parent ! ReportState(WorkerState.Ready)
    case QueryState => sender ! ReportState(WorkerState.Ready)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Ready, getOutputRowCount()))
  } orElse discardOthers

  def pausedBeforeStart:Receive =
    allowStashOrReleaseOutput orElse
    allowUpdateOutputLinking orElse
    allowModifyBreakpoints orElse
    disallowQueryTriggeredBreakpoints orElse[Any, Unit] {
    case QueryBreakpoint(id) =>
      val toReport = breakpoints.find(_.id == id)
      if(toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
        context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
        context.become(breakpointTriggered,discardOld = false)
        unstashAll()
      }
    case Resume =>
      context.parent ! ReportState(WorkerState.Ready)
      context.become(ready)
      unstashAll()
    case Pause => context.parent ! ReportState(WorkerState.Paused)
    case QueryState => sender ! ReportState(WorkerState.Paused)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Paused, getOutputRowCount()))
  } orElse discardOthers


  def paused:Receive =
    allowStashOrReleaseOutput orElse
    allowUpdateOutputLinking orElse
    allowModifyBreakpoints orElse
    disallowQueryTriggeredBreakpoints orElse[Any, Unit] {
    case Resume =>
        onResuming()
        onResumed()
        context.become(running)
        unstashAll()
    case Pause => context.parent ! ReportState(WorkerState.Paused)
    case QueryState => sender ! ReportState(WorkerState.Paused)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Paused, getOutputRowCount()))
    case QueryBreakpoint(id) =>
      val toReport = breakpoints.find(_.id == id)
      if(toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
        context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
        context.become(breakpointTriggered,discardOld = false)
        unstashAll()
      }
    case LocalBreakpointTriggered =>
      throw new AmberException("breakpoint triggered after pause")
  } orElse discardOthers


  def running:Receive=
    allowStashOrReleaseOutput orElse
    disallowUpdateOutputLinking orElse
    disallowModifyBreakpoints orElse
    disallowQueryBreakpoint orElse
    disallowQueryTriggeredBreakpoints orElse[Any, Unit]  {
    case ReportFailure(e) =>
      throw e
    case Pause =>
      log.info("received Pause message")
      onPausing()
    case LocalBreakpointTriggered =>
      log.info("receive breakpoint triggered")
      onBreakpointTriggered()
      context.become(paused)
      context.become(breakpointTriggered,discardOld = false)
      unstashAll()
    case ExecutionCompleted =>
      log.info("received complete")
      onCompleted()
      context.become(completed)
      unstashAll()
    case Resume => context.parent ! ReportState(WorkerState.Running)
    case QueryState => sender ! ReportState(WorkerState.Running)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Running, getOutputRowCount()))
  } orElse discardOthers


 def breakpointTriggered:Receive =
   allowStashOrReleaseOutput orElse
   allowUpdateOutputLinking orElse
   allowQueryBreakpoint orElse
   allowQueryTriggeredBreakpoints orElse[Any, Unit] {
   case AssignBreakpoint(bp) =>
     log.info("assign breakpoint: "+bp)
     sender ! Ack
     registerBreakpoint(bp)
     if(!breakpoints.exists(_.isDirty)){
       onPaused() //back to paused
       context.unbecome()
       unstashAll()
     }
   case RemoveBreakpoint(id) =>
     log.info("remove breakpoint: "+id)
     sender ! Ack
     removeBreakpoint(id)
     if(!breakpoints.exists(_.isDirty)){
       onPaused() //back to paused
       context.unbecome()
       unstashAll()
     }
   case QueryState => sender ! ReportState(WorkerState.LocalBreakpointTriggered)
   case QueryStatistics =>
     sender ! ReportStatistics(WorkerStatistics(WorkerState.LocalBreakpointTriggered, getOutputRowCount()))
   case DataMessage(_,_) | EndSending(_) => stash()
   case Resume | Pause => context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
   case LocalBreakpointTriggered => //discard this
  } orElse stashOthers

  def completed:Receive=
    allowStashOrReleaseOutput orElse
    disallowUpdateOutputLinking orElse
    allowModifyBreakpoints orElse
    allowQueryBreakpoint orElse[Any, Unit] {
    case QueryState => sender ! ReportState(WorkerState.Paused)
    case QueryStatistics =>
      sender ! ReportStatistics(WorkerStatistics(WorkerState.Completed, getOutputRowCount()))
    case QueryTriggeredBreakpoints => //skip this
    case ExecutionCompleted => //skip this as well
    case CollectSinkResults =>

    case msg =>
      if(sender == context.parent){
        sender ! ReportState(WorkerState.Completed)
      }
  }

}
