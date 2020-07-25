package Engine.Architecture.Worker

import java.util.concurrent.Executors

import Engine.Architecture.ReceiveSemantics.FIFOAccessPort
import Engine.Common.AmberException.{AmberException, BreakpointException}
import Engine.Common.AmberMessage.WorkerMessage._
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage.{QueryState, _}
import Engine.Common.AmberTag.{LayerTag, WorkerTag}
import Engine.Common.AmberTuple.{AmberTuple, Tuple}
import Engine.Common.{AdvancedMessageSending, ElidableStatement, TableMetadata, ThreadState, TupleProcessor}
import Engine.Operators.Sink.SimpleSinkProcessor
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.Breaks
import scala.annotation.elidable
import scala.annotation.elidable._
import scala.concurrent.duration._

object Processor {
  def props(processor:TupleProcessor,tag:WorkerTag): Props = Props(new Processor(processor,tag))
}

class Processor(val dataProcessor: TupleProcessor,val tag:WorkerTag) extends WorkerBase  {

  val dataProcessExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
  val processingQueue = new mutable.Queue[(LayerTag,Array[Tuple])]
  val input = new FIFOAccessPort()
  val aliveUpstreams = new mutable.HashSet[LayerTag]
  @volatile var dPThreadState: ThreadState.Value = ThreadState.Idle
  var processingIndex = 0
  var outputRowCount = 0

  @elidable(INFO) var processTime = 0L
  @elidable(INFO) var processStart = 0L

  override def onResuming(): Unit = {
    super.onResuming()
    if (processingQueue.nonEmpty) {
      dPThreadState = ThreadState.Running
      Future {
        processBatch()
      }(dataProcessExecutor)
    }else if(aliveUpstreams.isEmpty && dPThreadState != ThreadState.Completed){
      dPThreadState = ThreadState.Running
      Future{
        afterFinishProcessing()
      }(dataProcessExecutor)
    }
  }

  override def onCompleted(): Unit = {
    super.onCompleted()
    ElidableStatement.info{log.info("completed its job. total: {} ms, processing: {} ms",(System.nanoTime()-startTime)/1000000,processTime/1000000)}
  }

  override def getResultTuples(): mutable.MutableList[Tuple] = {
    this.dataProcessor match {
      case processor: SimpleSinkProcessor =>
        processor.getResultTuples()
      case _ =>
        mutable.MutableList()
    }
  }

  private[this] def waitProcessing:Receive={
    case ExecutionPaused =>
      context.become(paused)
      onPaused()
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

  def onSaveDataMessage(seq: Long, payload: Array[Tuple]): Unit = {
    input.preCheck(seq,payload,sender) match {
      case Some(batches) =>
        val currentEdge = input.actorToEdge(sender)
        synchronized {
          for (i <- batches)
            processingQueue += ((currentEdge, i))
        }
      case None =>
    }
  }

  def onSaveEndSending(seq: Long): Unit = {
    if(input.registerEnd(sender,seq)){
      synchronized {
        val currentEdge: LayerTag = input.actorToEdge(sender)
        processingQueue += ((currentEdge,null))
        if (dPThreadState == ThreadState.Idle) {
          dPThreadState = ThreadState.Running
          Future {
            processBatch()
          }(dataProcessExecutor)
        }
      }
    }
  }




  def onReceiveEndSending(seq: Long): Unit = {
    onSaveEndSending(seq)
  }

  def onReceiveDataMessage(seq: Long, payload: Array[Tuple]): Unit = {
    input.preCheck(seq,payload,sender) match{
      case Some(batches) =>
        val currentEdge = input.actorToEdge(sender)
        synchronized {
          for (i <- batches)
            processingQueue += ((currentEdge,i))
          if (dPThreadState == ThreadState.Idle) {
            dPThreadState = ThreadState.Running
            Future {
              processBatch()
            }(dataProcessExecutor)
          }
        }
      case None =>
    }
  }

  override def onPausing(): Unit = {
    super.onPausing()
    synchronized {
      //log.info("current state:" + dPThreadState)
      dPThreadState match{
        case ThreadState.Running =>
          context.become(waitProcessing)
          unstashAll()
        case ThreadState.Paused | ThreadState.Idle=>
          context.become(paused)
          unstashAll()
          onPaused()
        case _ =>
      }
    }
  }

  override def onInitialization(): Unit = {
    dataProcessor.initialize()
  }

  override def getOutputRowCount(): Int = {
    this.outputRowCount
  }

  final def activateWhenReceiveDataMessages:Receive = {
    case EndSending(_) | DataMessage(_,_) | RequireAck(_:EndSending) | RequireAck(_:DataMessage) =>
      stash()
      onStart()
      context.become(running)
      unstashAll()
  }

  final def disallowDataMessages:Receive = {
    case EndSending(_) | DataMessage(_,_) | RequireAck(_:EndSending) | RequireAck(_:DataMessage) =>
      throw new AmberException("not supposed to receive data messages at this time")
  }

  final def saveDataMessages:Receive = {
    case DataMessage(seq,payload) =>
      onSaveDataMessage(seq,payload)
    case RequireAck(msg: DataMessage) =>
      sender ! AckWithSequenceNumber(msg.sequenceNumber)
      onSaveDataMessage(msg.sequenceNumber,msg.payload)
    case EndSending(seq) =>
      onSaveEndSending(seq)
    case RequireAck(msg: EndSending) =>
      sender ! AckOfEndSending
      onSaveEndSending(msg.sequenceNumber)
  }

  final def receiveDataMessages:Receive = {
    case EndSending(seq) =>
      onReceiveEndSending(seq)
    case DataMessage(seq,payload) =>
      onReceiveDataMessage(seq,payload)
    case RequireAck(msg: EndSending) =>
      sender ! AckOfEndSending
      onReceiveEndSending(msg.sequenceNumber)
    case RequireAck(msg: DataMessage) =>
      sender ! AckWithSequenceNumber(msg.sequenceNumber)
      onReceiveDataMessage(msg.sequenceNumber,msg.payload)
  }

  final def allowUpdateInputLinking:Receive = {
    case UpdateInputLinking(inputActor,edgeID) =>
      sender ! Ack
      aliveUpstreams.add(edgeID)
      input.addSender(inputActor,edgeID)
  }

  final def disallowUpdateInputLinking:Receive = {
    case UpdateInputLinking(inputActor,edgeID) =>
      sender ! Ack
      throw new AmberException(s"update input linking of $edgeID is not allowed at this time")
  }

  final def reactOnUpstreamExhausted:Receive = {
    case ReportUpstreamExhausted(from) =>
      AdvancedMessageSending.nonBlockingAskWithRetry(context.parent,ReportWorkerPartialCompleted(tag,from),10,0)
  }


  override def postStop(): Unit = {
    processingQueue.clear()
    input.endToBeReceived.clear()
    input.actorToEdge.clear()
    input.seqNumMap.clear()
    input.endMap.clear()
    aliveUpstreams.clear()
  }



  override def ready: Receive = activateWhenReceiveDataMessages orElse allowUpdateInputLinking orElse super.ready

  override def pausedBeforeStart: Receive = saveDataMessages orElse allowUpdateInputLinking orElse super.pausedBeforeStart

  override def running: Receive = receiveDataMessages orElse disallowUpdateInputLinking orElse reactOnUpstreamExhausted orElse super.running

  override def paused: Receive = saveDataMessages orElse allowUpdateInputLinking orElse super.paused

  override def breakpointTriggered: Receive = saveDataMessages orElse allowUpdateInputLinking orElse super.breakpointTriggered

  override def completed: Receive = disallowDataMessages orElse disallowUpdateInputLinking orElse super.completed


  private[this] def beforeProcessingBatch(): Unit ={
  }

  private[this] def afterProcessingBatch(): Unit ={
    processingIndex = 0
    synchronized{
      processingQueue.dequeue()
      if(pausedFlag){
        dPThreadState = ThreadState.Paused
        self ! ExecutionPaused
      }else if(processingQueue.nonEmpty){
        Future {
          processBatch()
        }(dataProcessExecutor)
      }else if(aliveUpstreams.isEmpty){
        Future{
          afterFinishProcessing()
        }(dataProcessExecutor)
      }else{
        dPThreadState = ThreadState.Idle
      }
    }
  }


  private[this] def exitIfPaused(): Unit ={
    onInterrupted {
      dPThreadState = ThreadState.Paused
      self ! ExecutionPaused
      processTime += System.nanoTime()-processStart
    }
  }


  private[this] def afterFinishProcessing(): Unit ={
    Breaks.breakable {
      processStart=System.nanoTime()
      dataProcessor.noMore()
      while (dataProcessor.hasNext) {
        exitIfPaused()
        try {
          transferTuple(dataProcessor.next())
          outputRowCount += 1
        }catch{
          case e:BreakpointException =>
            synchronized {
              dPThreadState = ThreadState.LocalBreakpointTriggered
            }
            self ! LocalBreakpointTriggered
            processTime += System.nanoTime()-processStart
            Breaks.break()
          case e:Exception =>
            self ! ReportFailure(e)
            processTime += System.nanoTime()-processStart
            Breaks.break()
        }
      }
      onCompleting()
      try{
        dataProcessor.dispose()
      }catch{
        case e:Exception =>
          self ! ReportFailure(e)
          processTime += System.nanoTime()-processStart
          Breaks.break()
      }
      synchronized {
        dPThreadState = ThreadState.Completed
      }
      self ! ExecutionCompleted
      processTime += System.nanoTime()-processStart
    }
  }


  private[this] def processBatch(): Unit ={
    //log.info("enter processBatch "+i)
    Breaks.breakable {
      beforeProcessingBatch()
      processStart=System.nanoTime()
      val (from, batch) = synchronized{processingQueue.front}
      //check if there is tuple left to be outputted
      while(dataProcessor.hasNext){
        exitIfPaused()
        try {
          transferTuple(dataProcessor.next())
          outputRowCount += 1
        }catch{
          case e:BreakpointException =>
            synchronized {
              dPThreadState = ThreadState.LocalBreakpointTriggered
            }
            self ! LocalBreakpointTriggered
            processTime += System.nanoTime()-processStart
            Breaks.break()
          case e:Exception =>
            self ! ReportFailure(e)
            processTime += System.nanoTime()-processStart
            Breaks.break()
        }
      }
      if(batch == null){
        dataProcessor.onUpstreamExhausted(from)
        self ! ReportUpstreamExhausted(from)
        aliveUpstreams.remove(from)
      }else{
        dataProcessor.onUpstreamChanged(from)
        //no tuple remains, we continue
        while (processingIndex < batch.length) {
          exitIfPaused()
          try {
            dataProcessor.accept(batch(processingIndex))
          }catch{
            case e:Exception =>
              self ! ReportFailure(e)
              log.info(e.toString)
              processTime += System.nanoTime()-processStart
              Breaks.break()
            case other:Any =>
              println(other)
              println(batch(processingIndex))
          }
          processingIndex += 1
          while(dataProcessor.hasNext){
            exitIfPaused()
            try {
//              if(breakpoints.exists(_.isTriggered)){
//                log.info("break point triggered but it is not stopped")
//              }
              transferTuple(dataProcessor.next())
              outputRowCount += 1
            }catch{
              case e:BreakpointException =>
                synchronized {
                  dPThreadState = ThreadState.LocalBreakpointTriggered
                }
//                log.info("break point triggered")
                self ! LocalBreakpointTriggered
                processTime += System.nanoTime()-processStart
                Breaks.break()
              case e:Exception =>
                log.info(e.toString)
                self ! ReportFailure(e)
                processTime += System.nanoTime()-processStart
                Breaks.break()
            }
          }
        }
      }
      afterProcessingBatch()
      processTime += System.nanoTime()-processStart
    }
    //log.info("leave processBatch "+i)
  }
}