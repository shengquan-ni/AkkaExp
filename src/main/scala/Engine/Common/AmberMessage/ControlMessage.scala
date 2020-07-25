package Engine.Common.AmberMessage


object ControlMessage{

  final case class Start()

  final case class Pause()

  final case class Resume()

  final case class QueryState()

  final case class QueryStatistics()

  final case class CollectSinkResults()

  final case class LocalBreakpointTriggered()

  final case class RequireAck(msg:Any)

  final case class Ack()

  final case class AckWithInformation(info:Any)

  final case class AckWithSequenceNumber(sequenceNumber: Long)

  final case class AckOfEndSending()

  final case class StashOutput()

  final case class ReleaseOutput()
}
