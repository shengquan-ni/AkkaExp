package Engine.Architecture.Worker

import java.util.concurrent.Executors

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.{ExceptionBreakpoint, LocalBreakpoint}
import Engine.Common.AmberException.BreakpointException
import Engine.Common.{AdvancedMessageSending, ElidableStatement, ThreadState, TupleProducer}
import Engine.Common.AmberMessage.WorkerMessage._
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage._
import Engine.Common.AmberTag.WorkerTag
import Engine.Common.AmberTuple.Tuple
import Engine.FaultTolerance.Recovery.RecoveryPacket
import akka.actor.{ActorLogging, Props, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.Breaks
import scala.concurrent.duration._


object Generator {
  def props(producer:TupleProducer,tag:WorkerTag): Props = Props(new Generator(producer,tag))
}

class Generator(val dataProducer:TupleProducer,val tag:WorkerTag) extends WorkerBase with ActorLogging with Stash{

  val dataGenerateExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
  var isGeneratingFinished = false

  var generatedCount = 0L
  @elidable(INFO) var generateTime = 0L
  @elidable(INFO) var generateStart = 0L

  var outputRowCount = 0

  override def onResuming(): Unit = {
    super.onResuming()
    Future {
      Generate()
    }(dataGenerateExecutor)
  }

  override def onCompleted(): Unit = {
    super.onCompleted()
    ElidableStatement.info{log.info("completed its job. total: {} ms, generating: {} ms, generated {} tuples",(System.nanoTime()-startTime)/1000000,generateTime/1000000,generatedCount)}
  }

  override def onPaused(): Unit ={
    log.info(s"paused at $generatedCount , 0")
    context.parent ! RecoveryPacket(tag, generatedCount, 0)
    context.parent ! ReportState(WorkerState.Paused)
  }


  private[this] def waitGenerating:Receive={
    case ExecutionPaused =>
      onPaused()
      context.become(paused)
      unstashAll()
    case ReportFailure(e) =>
      throw e
    case ExecutionCompleted =>
      onCompleted()
      context.become(completed)
      unstashAll()
    case LocalBreakpointTriggered =>
      onBreakpointTriggered()
      context.become(paused)
      context.become(breakpointTriggered,discardOld = false)
      unstashAll()
    case QueryState => sender ! ReportState(WorkerState.Pausing)
    case QueryState => sender ! ReportStatistics(WorkerStatistics(WorkerState.Pausing, outputRowCount))
    case msg => stash()
  }

  override def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    var i = 0
    while (i < output.length) {
      output(i).accept(faultedTuple.tuple)
      i += 1
    }
    generatedCount+=1
  }

  override def getOutputRowCount(): Int = {
    this.outputRowCount
  }
  
  override def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    userFixedTuple = faultedTuple.tuple
  }

  override def onPausing(): Unit = {
    super.onPausing()
    synchronized{
      if(!isGeneratingFinished){
        ElidableStatement.finest{log.info("wait for generating thread")}
        context.become(waitGenerating)
        unstashAll()
      }else{
        onCompleted()
        context.become(completed)
        unstashAll()
      }
    }
  }

  override def onInitialization(recoveryInformation:Seq[(Long,Long)]): Unit = {
    super.onInitialization(recoveryInformation)
    dataProducer.initialize()
  }

  override def onInterrupted(operations: => Unit): Unit = {
    if(receivedRecoveryInformation.contains((generatedCount,0))){
      pausedFlag = true
      receivedRecoveryInformation.remove((generatedCount,0))
    }
    super.onInterrupted(operations)
  }


  override def onStart(): Unit = {
    Future {
      Generate()
    }(dataGenerateExecutor)
    super.onStart()
    context.become(running)
    unstashAll()
  }

  private[this] def beforeGenerating(): Unit ={
    if(userFixedTuple != null) {
      try {
        transferTuple(userFixedTuple, generatedCount)
        userFixedTuple = null
        generatedCount += 1
      } catch {
        case e: BreakpointException =>
          self ! LocalBreakpointTriggered
          generateTime += System.nanoTime() - generateStart
          Breaks.break()
        case e: Exception =>
          self ! ReportFailure(e)
          generateTime += System.nanoTime() - generateStart
          Breaks.break()
      }
    }
  }


  private[this] def exitIfPaused(): Unit ={
    onInterrupted{
      self ! ExecutionPaused
      generateTime += System.nanoTime()-generateStart
    }
  }

  private[this] def Generate(): Unit ={
    Breaks.breakable{
      generateStart = System.nanoTime()
      beforeGenerating()
      while(dataProducer.hasNext){
        exitIfPaused()
        var nextTuple:Tuple = null
        try{
          nextTuple = dataProducer.next()
        }catch{
          case e:Exception =>
            self ! LocalBreakpointTriggered
            breakpoints(0).triggeredTuple = nextTuple
            breakpoints(0).asInstanceOf[ExceptionBreakpoint].error = e
            breakpoints(0).triggeredTupleId = generatedCount
            generateTime += System.nanoTime()-generateStart
            Breaks.break()
        }
        try {
          transferTuple(nextTuple,generatedCount)
          generatedCount += 1
          outputRowCount += 1
        }catch{
          case e:BreakpointException =>
            self ! LocalBreakpointTriggered
            generateTime += System.nanoTime()-generateStart
            Breaks.break()
          case e:Exception =>
            self ! ReportFailure(e)
            generateTime += System.nanoTime()-generateStart
            Breaks.break()
        }
      }
      onCompleting()
      try{
        dataProducer.dispose()
      }catch{
        case e:Exception =>
          self ! ReportFailure(e)
          generateTime += System.nanoTime()-generateStart
          Breaks.break()
      }
      synchronized{
        isGeneratingFinished = true
        self ! ExecutionCompleted
      }
      generateTime += System.nanoTime()-generateStart
    }
  }
}
