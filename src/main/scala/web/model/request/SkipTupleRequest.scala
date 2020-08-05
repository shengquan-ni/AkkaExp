package web.model.request

import Engine.Architecture.Breakpoint.FaultedTuple
import akka.actor.ActorRef

case class SkipTupleRequest (actorRef:ActorRef, faultedTuple:FaultedTuple) extends TexeraWsRequest
