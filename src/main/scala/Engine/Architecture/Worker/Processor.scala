package Engine.Architecture.Worker

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.Executors

import Engine.Architecture.ReceiveSemantics.FIFOAccessPort
import Engine.Common.AmberException.{AmberException, BreakpointException}
import Engine.Common.AmberMessage.WorkerMessage._
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage.{QueryState, _}
import Engine.Common.AmberTag.{LayerTag, WorkerTag}
import Engine.Common.AmberTuple.{AmberTuple, Tuple}
import Engine.Common.{AdvancedMessageSending, ElidableStatement, TableMetadata, ThreadState, TupleProcessor}
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.Breaks
import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

object Processor {
  def props(processor:TupleProcessor,tag:WorkerTag): Props = Props(new Processor(processor,tag))
}

class Processor(val dataProcessor: TupleProcessor,val tag:WorkerTag) extends WorkerBase(tag)  {

  val dataProcessExecutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)
  val processingQueue = new mutable.Queue[(LayerTag,Array[Tuple])]
  val input = new FIFOAccessPort()
  val aliveUpstreams = new mutable.HashSet[LayerTag]
  @volatile var dPThreadState: ThreadState.Value = ThreadState.Idle
  var processingIndex = 0
  var totalBatchPutInInternalQueue = 0

  var tupleToIdentifyJoin:String = ""

  var flowControlActorsForJoin = new mutable.HashSet[ActorRef]()
  var count = 0
  var senderForJoin: ActorRef = null
  var internalQueueTimeStart = 0L
  var dpthreadProcessingTimeStart = 0L
  var totalBatchProcessed = 0
  val formatter = new SimpleDateFormat("HH:mm:ss.SSS z")
  var restartProcessingMap = new mutable.HashSet[(ActorRef,Int,ActorRef)]() // (principalStartingMitigation, mitigationCount, prevWorker)

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
    println(s" ${tag.getGlobalIdentity} ACTOR for Joining ${tupleToIdentifyJoin} TIME ####. total: ${(System.nanoTime()-startTime)/1000000} ms, processing: ${processTime/1000000} ms, batches ${totalBatchPutInInternalQueue} - ${formatter.format(new Date(System.currentTimeMillis()))}")
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
      if(tag.operator.contains("Join2-main/0")) {
        println("END accepted")
      }
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
    if(senderForJoin == null) {
      senderForJoin = sender
    }
    input.preCheck(seq,payload,sender) match{
      case Some(batches) =>
        val currentEdge = input.actorToEdge(sender)
        synchronized {
          for (i <- batches) {
            totalBatchPutInInternalQueue += 1
            processingQueue += ((currentEdge,i))

//            if(tag.operator.contains("Join2")) {
//              if(totalBatchPutInInternalQueue%1000 == 2) {
//                if(totalBatchPutInInternalQueue > 99) {
//                   println(s"Batches ${totalBatchPutInInternalQueue-1000}-${totalBatchPutInInternalQueue} put in queue in ${(System.nanoTime()-internalQueueTimeStart)/1000000}ms: ${tag.getGlobalIdentity}, ${formatter.format(new Date(System.currentTimeMillis()))}")
//
//                }
//                internalQueueTimeStart = System.nanoTime()
//              }
//            }

          }
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


  final def activateWhenReceiveDataMessages:Receive = {
    case EndSending(_) | DataMessage(_,_) | RequireAck(_:EndSending) | RequireAck(_:DataMessage) =>
      if(tag.operator.contains("Join2")) {
        println(s"ACTIVATED ${tag.getGlobalIdentity}")
      }

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
      if(tag.getGlobalIdentity.contains("sample-GroupBy3-localGroupBy")) {
        println(s"${tag.getGlobalIdentity} received END, needs ${input.endToBeReceived(input.endToBeReceived.keys.head).size}")
        //        for((k,v) <- input.endToBeReceived) {
        //          print(s"${k.getGlobalIdentity} needs ${v.size}, ")
        //        }
        //        println()
      }
      onReceiveEndSending(seq)
    case DataMessage(seq,payload) =>
      if(tag.operator.contains("Join2")) {
          flowControlActorsForJoin.add(sender)
      }
      onReceiveDataMessage(seq,payload)
    case RequireAck(msg: EndSending) =>
      sender ! AckOfEndSending
      if(tag.getGlobalIdentity.contains("sample-GroupBy3-localGroupBy")) {
        println(s"${tag.getGlobalIdentity} received END, needs ${input.endToBeReceived(input.endToBeReceived.keys.head).size}")
//        for((k,v) <- input.endToBeReceived) {
//          print(s"${k.getGlobalIdentity} needs ${v.size}, ")
//        }
//        println()
      }
      onReceiveEndSending(msg.sequenceNumber)
    case RequireAck(msg: DataMessage) =>
      if(tag.operator.contains("Join2")) {
        if(sender.toString().contains("Join1")) {
          flowControlActorsForJoin.add(sender)
        }
      }
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
      if(tag.getGlobalIdentity.contains("sample-Join2-main/0")) {
        println(s"Join2-0 reporting finish to principal")
      }
      AdvancedMessageSending.nonBlockingAskWithRetry(context.parent,ReportWorkerPartialCompleted(tag,from),10,0)
  }

  final def receiveSkewDetectionMessages:Receive = {
    case QuerySkewDetectionMetrics =>
      println()
      count += 1
      flowControlActorsForJoin.foreach( actor => actor ! ReportTime(tag, count))
      // sender ! ReportSkewMetrics(tag, new SkewMetrics(processingQueue.length, totalBatchPutInInternalQueue, input.stashedMessage(senderForJoin).size))
      sender ! (tag.getGlobalIdentity, SkewMetrics(processingQueue.length, totalBatchPutInInternalQueue, input.stashedMessage(senderForJoin).size))
  }

  final def receiveFlowControlSkewDetectionMessages:Receive = {
    case GetSkewMetricsFromFlowControl =>
      val flowControlActors: ArrayBuffer[ActorRef] = getFlowActors()
      var flowActorSkewMap: mutable.HashMap[ActorRef,(ActorRef, Int,Int)] = new mutable.HashMap[ActorRef,(ActorRef, Int,Int)]()
      flowControlActors.foreach(actor => {
        val (receiver,totalMessaegs,messagesToBeSent) = AdvancedMessageSending.blockingAskWithRetry(actor, GetSkewMetricsFromFlowControl, 3).asInstanceOf[(ActorRef,Int,Int)]
        flowActorSkewMap += (receiver -> (actor,totalMessaegs,messagesToBeSent))
      })

      sender ! (SkewMetricsFromPreviousWorker(flowActorSkewMap))
  }

  final def receiveBuildTableReplicationMsg:Receive = {
    case ReplicateBuildTable(to) =>
      if(tag.operator.contains("Join2")) {
        val hashTable:util.ArrayList[Any] = dataProcessor.getBuildHashTable()
        hashTable.forEach(map => to!ReceiveHashTable(map))
      }
      sender ! Ack
  }

  final def receiveHashTable: Receive = {
    case ReceiveHashTable(hashTable) =>
      if(tag.operator.contains("Join2")) {
        dataProcessor.renewHashTable(hashTable)
      }
  }

  final def receiveRouteUpdateMessages: Receive = {
    case UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker) =>
      val flowControlActors: ArrayBuffer[ActorRef] = getFlowActors()
      flowControlActors.foreach(actor => {
        actor ! UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker)
      })

  }

  final def receiveRestartFromPrincipal: Receive = {
    case RestartProcessingFreeWorker(principalRef, mitigationCount) =>
      println(s"RECEIVED RESTART from principal ${tag.getGlobalIdentity}")
      output.foreach(policy => {
        policy.resetPolicy()
        policy.propagateRestartForward(principalRef, mitigationCount)
      })

      // sender ! ReportState(WorkerState.Restarted)
      // AdvancedMessageSending.blockingAskWithRetry(context.parent, ReportState(WorkerState.Restarted), 3)
      context.become(restart)
      sender ! Ack
    /**
     * Once the freeWorker sends Ack, Principal runs the ReportStateLogic to mark freeWorker as restarted
     */
  }

  final def receiveRestartFromPrevWorker: Receive = {
    case RestartProcessing(principalRef, mitigationCount, senderActor,edgeID) =>
      // println(s"${tag.getGlobalIdentity} RECEIVED RESTART from previous worker")

      if(restartProcessingMap.contains((principalRef,mitigationCount,senderActor))) {
        // restart message should be idempotent
        sender ! Ack
      } else {
        restartProcessingMap.add((principalRef,mitigationCount,senderActor))
        // the below two lines are basically copied from UpdateInputLinking
        // the logic is that propagateRestartForward() calls the below two lines for all downstream workers from the free worker
        // But the below logic is called for free-worker separately when Join1 workers receive receiveRouteUpdateMessages()
        aliveUpstreams.add(edgeID)
        input.addSender(senderActor,edgeID)
        if(dPThreadState == ThreadState.Completed) {
          output.foreach(policy => {
            policy.resetPolicy()
            policy.propagateRestartForward(principalRef,mitigationCount)
          })
          AdvancedMessageSending.blockingAskWithRetry(context.parent, ReportState(WorkerState.Restarted), 3)
          context.become(restart)
        } else {
          output.foreach(policy => {
            policy.propagateRestartForward(principalRef,mitigationCount)
          })
        }
        sender ! Ack
      }
  }

  override def postStop(): Unit = {
    processingQueue.clear()
    input.endToBeReceived.clear()
    input.actorToEdge.clear()
    input.seqNumMap.clear()
    input.endMap.clear()
    aliveUpstreams.clear()
  }



  override def ready: Receive = activateWhenReceiveDataMessages orElse allowUpdateInputLinking orElse receiveRestartFromPrevWorker orElse super.ready

  def restart: Receive = activateWhenReceiveDataMessages orElse allowUpdateInputLinking

  override def pausedBeforeStart: Receive = saveDataMessages orElse allowUpdateInputLinking orElse super.pausedBeforeStart

  override def running: Receive = receiveDataMessages orElse disallowUpdateInputLinking orElse reactOnUpstreamExhausted orElse receiveSkewDetectionMessages orElse receiveBuildTableReplicationMsg orElse receiveRestartFromPrevWorker orElse super.running

  override def paused: Receive = saveDataMessages orElse allowUpdateInputLinking  orElse super.paused

  override def breakpointTriggered: Receive = saveDataMessages orElse allowUpdateInputLinking orElse super.breakpointTriggered

  override def completed: Receive = disallowDataMessages orElse receiveSkewDetectionMessages orElse receiveFlowControlSkewDetectionMessages orElse receiveRouteUpdateMessages orElse receiveHashTable orElse receiveRestartFromPrincipal orElse receiveRestartFromPrevWorker orElse super.completed


  private[this] def beforeProcessingBatch(): Unit ={
  }

  private[this] def afterProcessingBatch(): Unit ={
    if(tag.operator.contains("Join2")) {
      totalBatchProcessed += 1
//      if(totalBatchProcessed%1000 == 2) {
//        if(totalBatchProcessed > 99) {
//           println(s"Batches ${totalBatchProcessed-1000}-${totalBatchProcessed} PROCESSED in ${(System.nanoTime()-dpthreadProcessingTimeStart)/1000000}ms: ${tag.getGlobalIdentity}, ${formatter.format(new Date(System.currentTimeMillis()))}")
//
//        }
//        dpthreadProcessingTimeStart = System.nanoTime()
//      }
    }

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
      }else if(aliveUpstreams.isEmpty){ // i.e. if no layers are remaining
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
        if(tag.getGlobalIdentity.contains("sample-Join2-main/0")) {
          println(s"Join2-0 sending ReportUpstreamExhausted to itself - ${formatter.format(new Date(System.currentTimeMillis()))}")
        }
        self ! ReportUpstreamExhausted(from)
        aliveUpstreams.remove(from) //remove a particular layer
      }else{
        dataProcessor.onUpstreamChanged(from)
        //no tuple remains, we continue
        while (processingIndex < batch.length) {
          exitIfPaused()
          try {
            // println(s"DATA####: ${tag.operator} received ${batch(processingIndex).toString()}")
            if(tag.operator.contains("Join2") && tupleToIdentifyJoin.isEmpty) {
              tupleToIdentifyJoin = batch(processingIndex).toString()
            }
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