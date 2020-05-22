package Engine.Architecture.Worker

import java.util.concurrent.Executors

import Engine.Common.AmberException.BreakpointException
import Engine.Common.{AdvancedMessageSending, ElidableStatement, TupleProducer}
import Engine.Common.AmberMessage.WorkerMessage._
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage._
import Engine.Common.AmberTag.WorkerTag
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

class Generator(val dataProducer:TupleProducer,val tag:WorkerTag) extends WorkerBase(tag) with ActorLogging with Stash{

  val dataGenerateExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
  var isGeneratingFinished = false

  @elidable(INFO) var generatedCount = 0L
  @elidable(INFO) var generateTime = 0L
  @elidable(INFO) var generateStart = 0L

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
    case msg => stash()
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

  override def onInitialization(): Unit = {
    dataProducer.initialize()
  }

  override def onStart(): Unit = {
    Future {
      Generate()
    }(dataGenerateExecutor)
    super.onStart()
    context.become(running)
    unstashAll()
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
      println(s"Generate started for ${tag.getGlobalIdentity}")
      while(dataProducer.hasNext){
        exitIfPaused()
        try {
          transferTuple(dataProducer.next())
          generatedCount += 1
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
        println(s"Execution completed sent by ${tag.getGlobalIdentity}")
      }
      generateTime += System.nanoTime()-generateStart
    }
  }
}
