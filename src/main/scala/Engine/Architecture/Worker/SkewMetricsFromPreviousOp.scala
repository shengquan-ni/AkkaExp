package Engine.Architecture.Worker

import akka.actor.ActorRef
import scala.collection.mutable

/***
 *
 * @param flowActorSkewMap Key is the actor the data is being sent to, Value is the (flow control actor ref, totalQueueLength, SentTillNow)
 */
case class SkewMetricsFromPreviousWorker(flowActorSkewMap: mutable.HashMap[ActorRef,(ActorRef, Int,Int)])
