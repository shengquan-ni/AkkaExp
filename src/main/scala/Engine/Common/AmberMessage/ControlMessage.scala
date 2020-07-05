package Engine.Common.AmberMessage

import Engine.Common.AmberTag.{LayerTag, WorkerTag}
import akka.actor.ActorRef


object ControlMessage{

  final case class Start()

  final case class Pause()

  final case class Resume()

  final case class QueryState()

  final case class LocalBreakpointTriggered()

  final case class RequireAck(msg:Any)

  final case class Ack()

  final case class AckWithInformation(info:Any)

  final case class AckWithSequenceNumber(sequenceNumber: Long)

  final case class AckOfEndSending()

  final case class StashOutput()

  final case class ReleaseOutput()

  final case class QuerySkewDetectionMetrics()

  final case class GetSkewMetricsFromFlowControl()

  final case class TellJoin1Actor()

  final case class ReportTime(tag: WorkerTag, count: Integer)

  final case class ReplicateBuildTable(to:ActorRef)

  final case class RestartProcessing(principalRef: ActorRef, mitigationCount:Int, inputActor: ActorRef, fromLayer:LayerTag)

  final case class RestartProcessingToFlowControlActor(principalRef: ActorRef, mitigationCount:Int)

  final case class RestartProcessingFreeWorker(principalRef: ActorRef, mitigationCount: Int)

  final case class ReceiveHashTable(hashTable: Any)

  final case class UpdateRoutingForSkewMitigation(mostSkewedWorker: ActorRef, freeWorker: ActorRef)

  final case class AddFreeWorkerAsReceiver(mostSkewedWorker: ActorRef, freeWorker: ActorRef)

}
