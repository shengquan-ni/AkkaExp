package Engine.Architecture.LinkSemantics


import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePolicy, RoundRobinPolicy}
import Engine.Architecture.SendSemantics.Routees.DirectRoutee
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{UpdateInputLinking, UpdateOutputLinking}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class LocalOneToOne(from:ActorLayer, to:ActorLayer, batchSize:Int) extends LinkStrategy(from,to,batchSize)  {
  override def link()(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    assert(from.isBuilt && to.isBuilt && from.layer.length == to.layer.length)
    val froms: Map[String, Array[ActorRef]] = from.layer.groupBy(actor => actor.path.address.hostPort)
    val tos: Map[String, Array[ActorRef]] = to.layer.groupBy(actor =>actor.path.address.hostPort)
    assert(froms.keySet == tos.keySet && froms.forall(x => x._2.length == tos(x._1).length))
    froms.foreach(x =>{
      for(i <- x._2.indices){
        AdvancedMessageSending.blockingAskWithRetry(x._2(i),
          UpdateOutputLinking(new OneToOnePolicy(batchSize),tag, Array(new DirectRoutee(tos(x._1)(i)))),
          10)
      }
    })
  }
}
