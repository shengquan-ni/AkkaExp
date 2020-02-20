package Engine.Operators

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Controller.Workflow
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

abstract class OperatorMetadata(val tag: OperatorTag) extends Serializable {

  class Topology(val originalLayers:Array[ActorLayer],var links: Array[LinkStrategy],var dependencies:mutable.HashMap[LayerTag,mutable.HashSet[LayerTag]]) extends Serializable {
    assert(!dependencies.exists(x => x._2.contains(x._1)))
    var extraLayers:Array[ActorLayer] = Array()
    def layers: Array[ActorLayer] = originalLayers ++ extraLayers
  }

  lazy val topology:Topology = null

  def runtimeCheck(workflow:Workflow): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag,mutable.HashSet[LayerTag]]]] = {
    //do nothing by default
    None
  }

  def requiredShuffle:Boolean = false

  def getShuffleHashFunction(layerTag: LayerTag):Tuple => Int = ???

  def assignBreakpoint(topology:Array[ActorLayer], states:mutable.AnyRefMap[ActorRef,WorkerState.Value], breakpoint:GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter)

}
