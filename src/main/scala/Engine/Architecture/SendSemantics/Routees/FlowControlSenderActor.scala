package Engine.Architecture.SendSemantics.Routees


import Engine.Common.AmberMessage.ControlMessage.{AckOfEndSending, AckWithSequenceNumber, GetSkewMetricsFromFlowControl, Pause, ReportTime, RequireAck, Resume, UpdateRoutingForSkewMitigation}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.WorkerTag
import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props, Stash}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ArrayBuffer

object FlowControlSenderActor{
  def props(receiver:ActorRef): Props = Props(new FlowControlSenderActor(receiver))

  final val maxWindowSize = 64
  final val minWindowSize = 2
  final val expendThreshold = 4
  final val sendingTimeout:FiniteDuration = 30.seconds
//  final val activateBackPressureThreshold = 512
//  final val deactivateBackPressureThreshold = 32
  final val logicalTimeGap = 16
  final case class EndSendingTimedOut(receiver:ActorRef)
  final case class MessageTimedOut(receiver:ActorRef, seq:Long)
}


class FlowControlSenderActor(val receiver:ActorRef) extends Actor with Stash{
  import FlowControlSenderActor._

  implicit val timeout:Timeout = 1.second
  implicit val ec:ExecutionContext = context.dispatcher

  var exactlyCount = 0
  var ssThreshold = 16
  var windowSize = 2
  var maxSentSequenceNumber: mutable.HashMap[ActorRef,Long] = mutable.HashMap(receiver->0)
  var handleOfEndSending:mutable.HashMap[ActorRef,(Long,Cancellable)] = new mutable.HashMap[ActorRef,(Long,Cancellable)]()
//  var backPressureActivated = false

  val messagesOnTheWay = new mutable.HashMap[(ActorRef,Long),(Cancellable,DataMessage)]
  val messagesToBeSent = new mutable.Queue[DataMessage]

  var timeTaken = 0L
  var timeStart = 0L
  var countOfMessageTimedOut:Integer = 0
  var countOfMessagesReceived = 0
  val formatter = new SimpleDateFormat("HH:mm:ss.SSS z")

  var allReceivers: ArrayBuffer[ActorRef] =  ArrayBuffer(receiver)
  var sequenceNumberFromFlowActor: ArrayBuffer[Long] = ArrayBuffer(0)
  var currentReceiverIdx: Int = 0

  def incrementReceiverIdx():Unit = {
    currentReceiverIdx += 1
    currentReceiverIdx = currentReceiverIdx%allReceivers.size
  }

  override def receive: Receive = {
    case msg:DataMessage =>
      timeStart = System.nanoTime()
      countOfMessagesReceived += 1
      if(messagesOnTheWay.size < windowSize){
        val receiver = allReceivers(currentReceiverIdx)
        val seqNum = sequenceNumberFromFlowActor(currentReceiverIdx)
        msg.sequenceNumber = seqNum
        sequenceNumberFromFlowActor(currentReceiverIdx) += 1
        incrementReceiverIdx()
        maxSentSequenceNumber(receiver) = Math.max(maxSentSequenceNumber(receiver),seqNum)
        messagesOnTheWay += ((receiver,seqNum)->(context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(receiver,seqNum)),msg))
        receiver ! RequireAck(msg)
      }else{
        messagesToBeSent.enqueue(msg)
//        if(messagesToBeSent.size >= activateBackPressureThreshold){
//          //producer produces too much data, the network cannot handle it, activate back pressure
//          backPressureActivated = true
//          context.parent ! ActivateBackPressure
//        }
      }
      timeTaken += System.nanoTime()-timeStart
    case msg:EndSending =>
      timeStart = System.nanoTime()
      //always send end-sending message regardless the message queue size
      for(i<- 0 to allReceivers.size-1) {
        handleOfEndSending += (allReceivers(i)->(sequenceNumberFromFlowActor(i),context.system.scheduler.scheduleOnce(sendingTimeout,self,EndSendingTimedOut(allReceivers(i)))))
        msg.sequenceNumber = sequenceNumberFromFlowActor(i)
        allReceivers(i) ! RequireAck(msg)
      }
      timeTaken += System.nanoTime()-timeStart
    case AckWithSequenceNumber(seq) =>
      timeStart = System.nanoTime()
      if(messagesOnTheWay.contains(sender,seq)) {
        messagesOnTheWay(sender,seq)._1.cancel()
        messagesOnTheWay.remove(sender,seq)
        if (maxSentSequenceNumber(sender)-seq < logicalTimeGap) {
          if (windowSize < ssThreshold) {
            windowSize = Math.min(windowSize * 2, ssThreshold)
          } else {
            windowSize += 1
          }
        } else {
          ssThreshold /= 2
          windowSize = Math.max(minWindowSize,Math.min(ssThreshold,maxWindowSize))
        }
        if(messagesOnTheWay.size < windowSize && messagesToBeSent.nonEmpty){
          val msg = messagesToBeSent.dequeue()
          val receiver = allReceivers(currentReceiverIdx)
          val seqNum = sequenceNumberFromFlowActor(currentReceiverIdx)
          msg.sequenceNumber = seqNum
          sequenceNumberFromFlowActor(currentReceiverIdx) += 1
          incrementReceiverIdx()
          maxSentSequenceNumber(receiver) = Math.max(maxSentSequenceNumber(receiver),seqNum)
          messagesOnTheWay += ((receiver,seqNum)->(context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(receiver,seqNum)),msg))
          receiver ! RequireAck(msg)

//          maxSentSequenceNumber = Math.max(maxSentSequenceNumber,msg.sequenceNumber)
//          messagesOnTheWay(msg.sequenceNumber) = (context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(msg.sequenceNumber)),msg)
//          getReceiver() ! RequireAck(msg)
        }
      }
      timeTaken += System.nanoTime()-timeStart
    case AckOfEndSending =>
      if(handleOfEndSending != null){
        handleOfEndSending(sender)._2.cancel()
        handleOfEndSending(sender) = null
      }
    case EndSendingTimedOut(receiver) =>
      if(handleOfEndSending != null){
        handleOfEndSending(receiver) = (handleOfEndSending(receiver)._1,context.system.scheduler.scheduleOnce(sendingTimeout,self,EndSendingTimedOut(receiver)))
        receiver ! RequireAck(EndSending(handleOfEndSending(receiver)._1))
      }
    case MessageTimedOut(receiver, seq) =>
      timeStart = System.nanoTime()
      if(messagesOnTheWay.contains(receiver,seq)){
        countOfMessageTimedOut += 1
        //resend the data message
        val msg = messagesOnTheWay(receiver,seq)._2
        messagesOnTheWay((receiver,msg.sequenceNumber)) = (context.system.scheduler.scheduleOnce(sendingTimeout,self,MessageTimedOut(receiver,msg.sequenceNumber)),msg)
        receiver ! RequireAck(msg)
      }
      timeTaken += System.nanoTime()-timeStart
    case Resume =>
    case Pause => context.become(paused)
    case ReportTime(tag:WorkerTag, count:Integer) =>
      println(s"${count} FLOW sending to ${tag.getGlobalIdentity} time ${timeTaken/1000000}, messagesReceivedTillNow ${countOfMessagesReceived}, sent ${countOfMessagesReceived - messagesToBeSent.size} at ${formatter.format(new Date(System.currentTimeMillis()))}")

    case GetSkewMetricsFromFlowControl =>
      sender ! (allReceivers(0), countOfMessagesReceived, messagesToBeSent.size)

    case UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker) =>
      if(allReceivers.contains(mostSkewedWorker)) {
        // addReceiver(freeWorker)
      }
  }

  def addReceiver(worker: ActorRef): Unit = {
    allReceivers += worker
    sequenceNumberFromFlowActor += 0
    maxSentSequenceNumber += (worker -> 0)
  }

  final def paused:Receive ={
    case Pause =>
    case Resume =>
      context.become(receive)
      unstashAll()
    case msg => stash()
  }
}
