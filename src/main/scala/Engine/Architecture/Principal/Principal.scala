package Engine.Architecture.Principal

import Clustering.ClusterListener.GetAvailableNodeAddresses
import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberException.AmberException
import Engine.Common.AmberMessage.PrincipalMessage.{AssignBreakpoint, _}
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage._
import Engine.Common.AmberMessage.ControllerMessage.ReportGlobalBreakpointTriggered
import Engine.Common.AmberMessage.WorkerMessage
import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, QueryTriggeredBreakpoints, ReportUpstreamExhausted, ReportWorkerPartialCompleted, ReportedQueriedBreakpoint, ReportedTriggeredBreakpoints}
import Engine.Common.AmberTag.{LayerTag, OperatorTag}
import Engine.Common.{AdvancedMessageSending, AmberUtils, TableMetadata}
import Engine.Operators.OperatorMetadata
import akka.actor.{Actor, ActorLogging, ActorRef, Address, Cancellable, Props, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.after
import akka.pattern.ask

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}


object Principal {
  def props(metadata:OperatorMetadata): Props = Props(new Principal(metadata))
}


class Principal(val metadata:OperatorMetadata) extends Actor with ActorLogging with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout:Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  val tau:FiniteDuration = 1.second
  var workerLayers: Array[ActorLayer] = _
  var workerEdges: Array[LinkStrategy] = _
  var layerDependencies: mutable.HashMap[String,mutable.HashSet[String]] = _
  var workerStateMap: mutable.AnyRefMap[ActorRef,WorkerState.Value] = _
  var layerMetadata:Array[TableMetadata] = _
  var isUserPaused = false
  var globalBreakpoints = new mutable.AnyRefMap[String,GlobalBreakpoint]
  var periodicallyAskHandle:Cancellable = _
  var workersTriggeredBreakpoint:Iterable[ActorRef] = _
  var layerCompletedCounter:mutable.HashMap[LayerTag,Int] = _

  def allWorkerStates: Iterable[WorkerState.Value] = workerStateMap.values
  def allWorkers: Iterable[ActorRef] = workerStateMap.keys
  def unCompletedWorkerStates: Iterable[WorkerState.Value] = workerStateMap.filter(x => x._2!= WorkerState.Completed).values
  def unCompletedWorkers:Iterable[ActorRef] = workerStateMap.filter(x => x._2!= WorkerState.Completed).keys
  def availableNodes:Array[Address] = Await.result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses,5.seconds).asInstanceOf[Array[Address]]

  private def setWorkerState(worker:ActorRef, state:WorkerState.Value): Boolean = {
    assert(workerStateMap.contains(worker))
    //only set when state changes.
    if(workerStateMap(worker) != state) {
      if(WorkerState.ValidTransitions(workerStateMap(worker)).contains(state)) {
        workerStateMap(worker) = state
      }else if(WorkerState.SkippedTransitions(workerStateMap(worker)).contains(state)){
        log.info("Skipped worker state transition for worker{} from {} to {}",worker,workerStateMap(worker),state)
        workerStateMap(worker) = state
      }else{
        log.warning("Invalid worker state transition for worker{} from {} to {}",worker,workerStateMap(worker),state)
      }
      true
    } else false
  }

  final def whenAllUncompletedWorkersBecome(state:WorkerState.Value): Boolean = unCompletedWorkerStates.forall(_ == state)
  final def whenAllWorkersCompleted:Boolean = allWorkerStates.forall(_ == WorkerState.Completed)
  final def saveRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  final def ready:Receive = {
    case Start =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,Start,10,0))
    case WorkerMessage.ReportState(state) =>
      state match{
        case WorkerState.Running =>
          setWorkerState(sender,state)
          context.parent ! ReportState(PrincipalState.Running)
          context.become(running)
          unstashAll()
        case _ => //throw new AmberException("Invalid worker state received!")
      }
    case GetInputLayer => sender ! workerLayers.head.clone()
    case GetOutputLayer => sender ! workerLayers.last.clone()
    case QueryState => sender ! ReportState(PrincipalState.Ready)
    case Resume => context.parent ! ReportState(PrincipalState.Ready)
    case AssignBreakpoint(breakpoint) =>
      globalBreakpoints(breakpoint.id) = breakpoint
      log.info("assign breakpoint: "+breakpoint.id)
      metadata.assignBreakpoint(workerLayers,workerStateMap,breakpoint)
      sender ! Ack
    case Pause =>
      allWorkers.foreach(worker => worker ! Pause)
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(0.milliseconds,30.seconds,self,EnforceStateCheck)
      context.become(pausing)
      unstashAll()
    case msg => stash()
  }

  final def running:Receive = {
    case WorkerMessage.ReportState(state) =>
      //log.info("running: "+ sender +" to "+ state)
      if(setWorkerState(sender,state)) {
        state match {
          case WorkerState.LocalBreakpointTriggered =>
            if(whenAllUncompletedWorkersBecome(WorkerState.LocalBreakpointTriggered)){
              //only one worker and it triggered breakpoint
              saveRemoveAskHandle()
              periodicallyAskHandle = context.system.scheduler.schedule(0.milliseconds,30.seconds,self,EnforceStateCheck)
              workersTriggeredBreakpoint = allWorkers
              context.parent ! ReportState(PrincipalState.CollectingBreakpoints)
              context.become(collectingBreakpoints)
            }else{
              //no tau involved since we know a very small tau works best
              unCompletedWorkers.foreach(worker => worker ! Pause)
              saveRemoveAskHandle()
              periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
              context.become(pausing)
              unstashAll()
            }
          case WorkerState.Paused =>
            //WARN: possibly dropped LocalBreakpointTriggered message OR backpressure?
            log.warning(sender + " paused itself with unknown reason")
            //try to resume it
            sender ! Resume
          case WorkerState.Completed =>
            log.info(sender+" completed")
            if (whenAllWorkersCompleted) {
              context.parent ! ReportState(PrincipalState.Completed)
              context.become(completed)
              unstashAll()
            }
          case _ => //skip others for now
        }
      }
    case Pause =>
      //single point pause: pause itself
      if(sender != self){
        isUserPaused = true
      }
      allWorkers.foreach(worker => worker ! Pause)
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.become(pausing)
      unstashAll()
    case ReportWorkerPartialCompleted(worker,layer) =>
      sender ! Ack
      AdvancedMessageSending.nonBlockingAskWithRetry(context.parent, ReportPrincipalPartialCompleted(worker,layer),10,0)
      if(layerCompletedCounter.contains(layer)){
        layerCompletedCounter(layer) -= 1
        if(layerCompletedCounter(layer) == 0){
          layerCompletedCounter -= layer
          AdvancedMessageSending.nonBlockingAskWithRetry(context.parent, ReportPrincipalPartialCompleted(metadata.tag,layer),10,0)
        }
      }
    case Resume => context.parent ! ReportState(PrincipalState.Running)
    case QueryState => sender ! ReportState(PrincipalState.Running)
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }


  final lazy val allowedStatesOnPausing: Set[WorkerState.Value] = Set(WorkerState.Completed, WorkerState.Paused, WorkerState.LocalBreakpointTriggered)

  final def pausing:Receive={
    case EnforceStateCheck =>
      for((k,v) <- workerStateMap){
        if(!allowedStatesOnPausing.contains(v)){
          k ! QueryState
        }
      }
    case WorkerMessage.ReportState(state) =>
      log.info("pausing: "+ sender +" to "+ state)
      if(!allowedStatesOnPausing(state) && state != WorkerState.Pausing){
        sender ! Pause
      }else if(setWorkerState(sender,state)) {
        if (whenAllWorkersCompleted) {
          saveRemoveAskHandle()
          context.parent ! ReportState(PrincipalState.Completed)
          context.become(completed)
          unstashAll()
        }else if (whenAllUncompletedWorkersBecome(WorkerState.Paused)) {
          saveRemoveAskHandle()
          context.parent ! ReportState(PrincipalState.Paused)
          context.become(paused)
          unstashAll()
        }else if (unCompletedWorkerStates.forall(x => x == WorkerState.Paused || x == WorkerState.LocalBreakpointTriggered)) {
          workersTriggeredBreakpoint = workerStateMap.filter(_._2 == WorkerState.LocalBreakpointTriggered).keys
          saveRemoveAskHandle()
          periodicallyAskHandle = context.system.scheduler.schedule(1.milliseconds, 30.seconds, self, EnforceStateCheck)
          context.parent ! ReportState(PrincipalState.CollectingBreakpoints)
          context.become(collectingBreakpoints)
          unstashAll()
        }
      }
    case QueryState => sender ! ReportState(PrincipalState.Pausing)
    case Pause =>
      if(sender != self){
        isUserPaused = true
      }
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }


  final def collectingBreakpoints:Receive={
    case EnforceStateCheck =>
      workersTriggeredBreakpoint.foreach(x => x ! QueryTriggeredBreakpoints)//query all
    case WorkerMessage.ReportState(state) =>
      log.info("collecting: "+ sender +" to "+ state)
      if(setWorkerState(sender,state)) {
          if(unCompletedWorkerStates.forall(_ == WorkerState.Paused)){
            //all breakpoint resolved, it's safe to report to controller and then Pause(on triggered, or user paused) else Resume
            for(i <- globalBreakpoints.values.filter(_.isTriggered)){
              isUserPaused = true //upgrade pause
              val report = i.report()
              log.info(report)
              context.parent ! ReportGlobalBreakpointTriggered(report)
            }
            saveRemoveAskHandle()
            context.parent ! ReportState(PrincipalState.Paused)
            context.become(paused)
            unstashAll()
            if(!isUserPaused){
              log.info("no global breakpoint triggered, continue")
              self ! Resume
            }else{
              log.info("user paused or global breakpoint triggered, pause")
            }
          }
      }
    case ReportedTriggeredBreakpoints(bps) =>
      bps.foreach(x =>{
        val bp = globalBreakpoints(x.id)
        bp.accept(sender,x)
        if(bp.needCollecting) {
          //is not fully collected
          bp.collect()
        }else if (bp.isRepartitionRequired) {
          //fully collected, but need repartition (e.g. count not reach target number)
          //OR need Reset
          metadata.assignBreakpoint(workerLayers, workerStateMap, bp)
        } else if (bp.isCompleted) {
          //fully collected and reach the target
          bp.remove()
        }
      })
    case ReportedQueriedBreakpoint(bp) =>
      val gbp = globalBreakpoints(bp.id)
      if(gbp.accept(sender,bp) && !gbp.needCollecting){
        if (gbp.isRepartitionRequired) {
          //fully collected, but need repartition (count not reach target number)
          metadata.assignBreakpoint(workerLayers, workerStateMap, gbp)
        } else if (gbp.isCompleted) {
          //fully collected and reach the target
          gbp.remove()
        }
      }
    case Pause =>
      if(sender != self){
        isUserPaused = true
      }
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }




  final lazy val allowedStatesOnResuming: Set[WorkerState.Value] = Set(WorkerState.Running,WorkerState.Ready,WorkerState.Completed)

  final def resuming:Receive={
    case EnforceStateCheck =>
      for((k,v) <- workerStateMap){
        if(!allowedStatesOnResuming.contains(v)){
          k ! QueryState
        }
      }
    case WorkerMessage.ReportState(state) =>
      log.info("resuming: "+ sender +" to "+ state)
      if(!allowedStatesOnResuming.contains(state)){
        sender ! Resume
      }else if(setWorkerState(sender,state)) {
        if(whenAllWorkersCompleted){
          saveRemoveAskHandle()
          context.parent ! ReportState(PrincipalState.Completed)
          context.become(completed)
          unstashAll()
        }else if(allWorkerStates.forall(_ != WorkerState.Paused)){
          saveRemoveAskHandle()
          if(allWorkerStates.exists(_ != WorkerState.Ready)) {
            context.parent ! ReportState(PrincipalState.Running)
            context.become(running)
          } else{
            context.parent ! ReportState(PrincipalState.Ready)
            context.become(ready)
          }
          unstashAll()
        }
      }
    case QueryState => sender ! ReportState(PrincipalState.Resuming)
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }


  final def paused:Receive={
    case Resume =>
      isUserPaused = false //reset
      assert(unCompletedWorkerStates.nonEmpty)
      unCompletedWorkers.foreach(worker => worker ! Resume)
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.become(resuming)
      unstashAll()
    case AssignBreakpoint(breakpoint) =>
      sender ! Ack
      globalBreakpoints(breakpoint.id) = breakpoint
      metadata.assignBreakpoint(workerLayers,workerStateMap,breakpoint)
    case GetInputLayer => sender ! workerLayers.head.layer
    case GetOutputLayer => sender ! workerLayers.last.layer
    case Pause => context.parent ! ReportState(PrincipalState.Paused)
    case QueryState => sender ! ReportState(PrincipalState.Paused)
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }

  final def completed:Receive={
    case msg =>
      log.info("received {} from {} after complete",msg,sender)
      if(sender == context.parent){
        sender ! ReportState(PrincipalState.Completed)
      }
  }


  final override def receive: Receive = {
    case AckedPrincipalInitialization(prev:Array[(OperatorMetadata,ActorLayer)]) =>
      workerLayers = metadata.topology.layers
      workerEdges = metadata.topology.links
      val all = availableNodes
      if(workerEdges.isEmpty){
        workerLayers.foreach(x => x.build(prev,all))
      }else{
        val inLinks: Map[ActorLayer, Set[ActorLayer]] = workerEdges.groupBy(x => x.to).map(x=> (x._1,x._2.map(_.from).toSet))
        var currentLayer:Iterable[ActorLayer] = workerEdges.filter(x => workerEdges.forall(_.to != x.from)).map(_.from)
        currentLayer.foreach(x => x.build(prev,all))
        currentLayer = inLinks.filter(x => x._2.forall(_.isBuilt)).keys
        while(currentLayer.nonEmpty){
          currentLayer.foreach(x => x.build(inLinks(x).map(x =>(null,x)).toArray,all))
          currentLayer = inLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
        }
      }
      layerCompletedCounter = mutable.HashMap(prev.map(x => x._2.tag -> x._2.layer.length).toSeq:_*)
      workerStateMap = mutable.AnyRefMap(workerLayers.flatMap(x => x.layer).map((_, WorkerState.Uninitialized)).toMap.toSeq:_*)
      allWorkers.foreach(x => x ! AckedWorkerInitialization)
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.become(initializing)
      unstashAll()
      sender ! AckWithInformation(metadata)
    case QueryState => sender ! ReportState(PrincipalState.Uninitialized)
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }


  final def initializing: Receive ={
    case EnforceStateCheck =>
      for((k,v) <- workerStateMap){
        if(v != WorkerState.Ready){
          k ! QueryState
        }
      }
    case WorkerMessage.ReportState(state) =>
      if(state != WorkerState.Ready){
         sender ! AckedWorkerInitialization
      }else if(setWorkerState(sender,state)){
        if(whenAllUncompletedWorkersBecome(WorkerState.Ready)){
          saveRemoveAskHandle()
          workerEdges.foreach(x => x.link())
          context.parent ! ReportState(PrincipalState.Ready)
          context.become(ready)
          unstashAll()
        }
      }
    case QueryState => sender ! ReportState(PrincipalState.Initializing)
    case msg =>
      log.info("stashing: "+ msg)
      stash()
  }

}
