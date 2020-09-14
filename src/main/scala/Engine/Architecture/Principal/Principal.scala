package Engine.Architecture.Principal

import java.text.SimpleDateFormat

import Clustering.ClusterListener.GetAvailableNodeAddresses
import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.{SkewMetrics, SkewMetricsFromPreviousWorker, WorkerState}
import Engine.Common.AmberException.AmberException
import Engine.Common.AmberMessage.PrincipalMessage.{AssignBreakpoint, _}
import Engine.Common.AmberMessage.StateMessage._
import Engine.Common.AmberMessage.ControlMessage._
import Engine.Common.AmberMessage.ControllerMessage.ReportGlobalBreakpointTriggered
import Engine.Common.AmberMessage.WorkerMessage
import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, QueryTriggeredBreakpoints, ReportSkewMetrics, ReportUpstreamExhausted, ReportWorkerPartialCompleted, ReportedQueriedBreakpoint, ReportedTriggeredBreakpoints, UpdateInputsLinking}
import Engine.Common.AmberTag.{LayerTag, OperatorTag, WorkerTag}
import Engine.Common.{AdvancedMessageSending, AmberUtils, Constants, TableMetadata}
import Engine.Operators.OperatorMetadata
import akka.actor.{Actor, ActorLogging, ActorRef, Address, Cancellable, Props, Stash}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.after
import akka.pattern.ask
import com.google.common.base.Stopwatch

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.util.Date


object Principal {
  def props(metadata:OperatorMetadata): Props = Props(new Principal(metadata))
}


class Principal(val metadata:OperatorMetadata) extends Actor with ActorLogging with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout:Timeout = 10.seconds
  implicit val logAdapter: LoggingAdapter = log

  val tau:FiniteDuration = Constants.defaultTau
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
  val timer = new Stopwatch()
  val stage1Timer = new Stopwatch()
  val stage2Timer = new Stopwatch()
  var skewQueryStartTime = 0L
  val formatter = new SimpleDateFormat("HH:mm:ss.SSS z")
  var join1Principal: ActorRef = null
  var join1OperatorTag: OperatorTag = null
  var countOfSkewQuery: Int = 1
  var mitigationCount: Int = 0

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
      // println(s"Principal ${metadata.tag.operator} sending START to its workers")
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,Start,10,0))
    case WorkerMessage.ReportState(state) =>
      state match{
        case WorkerState.Running =>
          setWorkerState(sender,state)
          context.parent ! ReportState(PrincipalState.Running)
          context.become(running)
          timer.start()
          stage1Timer.start()
          unstashAll()
        case _ => //throw new AmberException("Invalid worker state received!")
      }
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,StashOutput,10,0))
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,ReleaseOutput,10,0))
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
      // log.info("running: "+ sender +" to "+ state)
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
              if(!stage2Timer.isRunning){
                stage1Timer.stop()
                stage2Timer.start()
              }
              context.system.scheduler.scheduleOnce(tau,() => unCompletedWorkers.foreach(worker => worker ! Pause))
              saveRemoveAskHandle()
              periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
              context.become(pausing)
              unstashAll()
            }
          case WorkerState.Restarted =>
            sender ! Ack
            workerStateMap(sender) = WorkerState.Restarted
            if(layerCompletedCounter.keys.size != 1) {
              println(s"Error!! layerCompletedCounter size is ${layerCompletedCounter.keys.size} only 1 layer should remain. Inner table layer should have already finished.")
            }
            val layerTag: LayerTag = layerCompletedCounter.keys.head
            layerCompletedCounter(layerTag) += 1
          case WorkerState.Paused =>
            //WARN: possibly dropped LocalBreakpointTriggered message OR backpressure?
            log.warning(sender + " paused itself with unknown reason")
            //try to resume it
            sender ! Resume
          case WorkerState.Completed =>
            if (whenAllWorkersCompleted) {
              timer.stop()
              log.info(metadata.tag.toString+" completed! Time Elapsed: "+timer.toString())
              context.parent ! ReportState(PrincipalState.Completed)
              context.become(completed)
              unstashAll()
            } else {
              if(metadata.tag.operator.contains("Join2")) {
                if(mitigationCount<0 && (System.nanoTime()-skewQueryStartTime)/1000000 > 100) {
                  val mostSkewedWorker: ActorRef = SkewDetection()
                  SkewMitigation(self, mitigationCount, mostSkewedWorker, sender)
                  countOfSkewQuery += 1
                  skewQueryStartTime = System.nanoTime()
                  mitigationCount += 1
                }
              }
            }
          case _ => //skip others for now
        }
      }
    case ReportSkewMetrics(tag, skewMetric) =>
      println(s"${tag.getGlobalIdentity} reports current queue ${skewMetric.unprocessedQueueLength}, cumulative queue ${skewMetric.totalPutInInternalQueue}, stash ${skewMetric.stashedBatches} at ${formatter.format(new Date(System.currentTimeMillis()))}")
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
      if(worker.getGlobalIdentity.contains("sample-Join2-main/0")) {
        println(s"Principal receives Join2 - 0 request")
      }
      if(layerCompletedCounter.contains(layer)){
        layerCompletedCounter(layer) -= 1
        if(layerCompletedCounter(layer) == 0){
          layerCompletedCounter -= layer
          // This is used to control the stage by stage execution especially for Join where the stage belonging to inner table should finish first
          AdvancedMessageSending.nonBlockingAskWithRetry(context.parent, ReportPrincipalPartialCompleted(metadata.tag,layer),10,0)
        }
      }
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,StashOutput,10,0))
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,ReleaseOutput,10,0))
    case Resume => context.parent ! ReportState(PrincipalState.Running)
    case QueryState => sender ! ReportState(PrincipalState.Running)
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

  def SkewDetection()(implicit sender:ActorRef): ActorRef = {
    var workersSkewMap: mutable.HashMap[ActorRef,(String,SkewMetrics)] = new mutable.HashMap[ActorRef,(String,SkewMetrics)]()

    val tagAndMetricsArr =  AdvancedMessageSending.blockingAskWithRetry(unCompletedWorkers.toArray, QuerySkewDetectionMetrics, 3).asInstanceOf[ArrayBuffer[(String,SkewMetrics)]]
    var i=0
    for (worker <- unCompletedWorkers) {
      workersSkewMap += (worker -> tagAndMetricsArr(i))
      i += 1
    }

    if(join1Principal == null) {
      val x  = AdvancedMessageSending.blockingAskWithRetry(context.parent, TellJoin1Actor, 3).asInstanceOf[(ActorRef, OperatorTag)]
      join1Principal = x._1
      join1OperatorTag = x._2
    }
    // Map of [Join2Worker -> Array(Join1FlowControlActor, TotalToBeSent, MessagesYetToBeSent)]
    val flowControlSkewMap: mutable.HashMap[ActorRef,ArrayBuffer[(ActorRef,Int,Int)]] = AdvancedMessageSending.blockingAskWithRetry(join1Principal, QuerySkewDetectionMetrics, 3).asInstanceOf[mutable.HashMap[ActorRef,ArrayBuffer[(ActorRef,Int,Int)]]]
    println()
    var mostSkewedWorker: ActorRef = null
    var mostDataToProcess: Int = -1

    unCompletedWorkers.foreach(worker => {
      var sum2 = 0
      flowControlSkewMap.getOrElse(worker, new ArrayBuffer[(ActorRef,Int,Int)]()).foreach(metric=> {
        // Find total # of messages the worker is yet to receive
        sum2 += metric._3
      })
      if(sum2+workersSkewMap.getOrElse(worker, ("",SkewMetrics(0,0,0)))._2.totalPutInInternalQueue > mostDataToProcess) {
        mostDataToProcess = sum2+workersSkewMap.getOrElse(worker, ("",SkewMetrics(0,0,0)))._2.totalPutInInternalQueue
        mostSkewedWorker = worker
      }
      println(s"${countOfSkewQuery} SKEW METRICS FOR ${workersSkewMap.getOrElse(worker,("",SkewMetrics(0,0,0)))._1}- ${workersSkewMap.getOrElse(worker, ("",SkewMetrics(0,0,0)))._2.totalPutInInternalQueue} and ${sum2}")
    })
    println(s"${countOfSkewQuery} Most skewed worker is ${workersSkewMap.getOrElse(mostSkewedWorker,("",SkewMetrics(0,0,0)))._1}")
    mostSkewedWorker
  }

  def SkewMitigation(principalRef: ActorRef, mitigationCount: Int, mostSkewedWorker: ActorRef, freeWorker:ActorRef): Unit = {
    AdvancedMessageSending.blockingAskWithRetry(freeWorker, RestartProcessingFreeWorker(principalRef, mitigationCount), 3)
    println(s"Restart message propagated - ${formatter.format(new Date(System.currentTimeMillis()))}")

    // below block says that freeWorker is restarted.
    workerStateMap(freeWorker) = WorkerState.Restarted
    if(layerCompletedCounter.keys.size != 1) {
      println(s"Error!! layerCompletedCounter size is ${layerCompletedCounter.keys.size} only 1 layer should remain. Inner table layer should have already finished.")
    }
    val layerTag: LayerTag = layerCompletedCounter.keys.head
    layerCompletedCounter(layerTag) += 1

    AdvancedMessageSending.blockingAskWithRetry(mostSkewedWorker, ReplicateBuildTableAndUpdateInputLinking(freeWorker, join1OperatorTag), 3)
    println(s"Replication done and Input updating done - ${formatter.format(new Date(System.currentTimeMillis()))}")

//    println(s"GETTING INFO FROM JOIN1 PRINCIPAL")
//    val (inputsToBeUpdated: ArrayBuffer[ActorRef], lyTag:LayerTag) = AdvancedMessageSending.blockingAskWithRetry(join1Principal, UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker), 3)
//
//    println(s"SENDING INFO TO FREE WORKER")
//    AdvancedMessageSending.blockingAskWithRetry(freeWorker, UpdateInputsLinking(inputsToBeUpdated, lyTag), 3)

    println(s"OPENING THE FLOODGATES ${formatter.format(new Date(System.currentTimeMillis()))}")
    join1Principal ! AddFreeWorkerAsReceiver(mostSkewedWorker, freeWorker)
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
      //log.info("pausing: "+ sender +" to "+ state)
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
      //log.info("stashing: "+ msg)
      stash()
  }


  final def collectingBreakpoints:Receive={
    case EnforceStateCheck =>
      workersTriggeredBreakpoint.foreach(x => x ! QueryTriggeredBreakpoints)//query all
    case WorkerMessage.ReportState(state) =>
      //log.info("collecting: "+ sender +" to "+ state)
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
              stage2Timer.stop()
              stage1Timer.start()
              self ! Resume
            }else{
              stage2Timer.stop()
              log.info("user paused or global breakpoint triggered, pause. Stage1 cost = "+stage1Timer.toString()+" Stage2 cost ="+stage2Timer.toString())

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
      //log.info("stashing: "+ msg)
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
      //log.info("resuming: "+ sender +" to "+ state)
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
      //log.info("stashing: "+ msg)
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
      //log.info("stashing: "+ msg)
      stash()
  }

  final def completed:Receive={
    case StashOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,StashOutput,10,0))
    case ReleaseOutput =>
      sender ! Ack
      allWorkers.foreach(worker => AdvancedMessageSending.nonBlockingAskWithRetry(worker,ReleaseOutput,10,0))
    case QuerySkewDetectionMetrics =>
      // the below takes place in Join1

      // Map of [Join2Worker -> Array(Join1FlowControlActor, TotalToBeSent, MessagesYetToBeSent)]
      var join2ActorsSkewMap: mutable.HashMap[ActorRef,ArrayBuffer[(ActorRef,Int,Int)]] = new mutable.HashMap[ActorRef,ArrayBuffer[(ActorRef,Int,Int)]]()

      val skewMetricsFromPrevWorkersArr = AdvancedMessageSending.blockingAskWithRetry(allWorkers.toArray, GetSkewMetricsFromFlowControl, 3).asInstanceOf[ArrayBuffer[SkewMetricsFromPreviousWorker]]
      var i=0
      for( _ <- allWorkers) {
        for((key,value) <- skewMetricsFromPrevWorkersArr(i).flowActorSkewMap) {
          var oldValue: ArrayBuffer[(ActorRef,Int,Int)] = join2ActorsSkewMap.getOrElse(key, new ArrayBuffer[(ActorRef,Int,Int)]())
          oldValue += value
          join2ActorsSkewMap.put(key,oldValue)
        }
        i += 1
      }

//      allWorkers.foreach(worker => {
//        // Map of [Join2Worker -> (Join1FlowControlActor, TotalToBeSent, MessagesYetToBeSent)]
//        val skewMetricsFromPrevWorker: SkewMetricsFromPreviousWorker = AdvancedMessageSending.blockingAskWithRetry(worker, GetSkewMetricsFromFlowControl, 3).asInstanceOf[SkewMetricsFromPreviousWorker]
//        for((key,value) <- skewMetricsFromPrevWorker.flowActorSkewMap) {
//          var oldValue: ArrayBuffer[(ActorRef,Int,Int)] = join2ActorsSkewMap.getOrElse(key, new ArrayBuffer[(ActorRef,Int,Int)]())
//          oldValue += value
//          join2ActorsSkewMap.put(key,oldValue)
//        }
//      })
      sender ! join2ActorsSkewMap
    case UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker) =>
      println(s"Join1 principal got UpdateRouting message")
      var ret = new ArrayBuffer[ActorRef]()
      var layTag: LayerTag = null
      allWorkers.foreach(worker => {
        val (inputsToBeUpdated: ArrayBuffer[ActorRef], layerTag: LayerTag) = AdvancedMessageSending.blockingAskWithRetry(worker, UpdateRoutingForSkewMitigation(mostSkewedWorker,freeWorker), 3)
        ret.appendAll(inputsToBeUpdated)
        layTag = layerTag
      })
      sender ! (ret,layTag)

    case AddFreeWorkerAsReceiver(mostSkewedWorker,freeWorker) =>
      println(s"JOIN1 principal now adding free workers as receivers")
      allWorkers.foreach(worker => {
        worker ! AddFreeWorkerAsReceiver(mostSkewedWorker,freeWorker)
      })
    case msg =>
      //log.info("received {} from {} after complete",msg,sender)
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
        // if a worker has only one layer, then build it according to the previous operator's last layer
        workerLayers.foreach(x => x.build(prev,all))
      }else{
        // else build all the layers in a topological order
        val inLinks: Map[ActorLayer, Set[ActorLayer]] = workerEdges.groupBy(x => x.to).map(x=> (x._1,x._2.map(_.from).toSet))
        var currentLayer:Iterable[ActorLayer] = workerEdges.filter(x => workerEdges.forall(_.to != x.from)).map(_.from)
        currentLayer.foreach(x => x.build(prev,all))
        currentLayer = inLinks.filter(x => x._2.forall(_.isBuilt)).keys
        while(currentLayer.nonEmpty){
          currentLayer.foreach(x => x.build(inLinks(x).map(y =>(null,y)).toArray,all))
          currentLayer = inLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
        }
      }
      // layerCompletedCounter = mutable.HashMap(prev.map(x => x._2.tag -> x._2.layer.length).toSeq:_*)
      layerCompletedCounter = mutable.HashMap(prev.map(x => x._2.tag -> workerLayers.head.layer.length).toSeq:_*)
      workerStateMap = mutable.AnyRefMap(workerLayers.flatMap(x => x.layer).map((_, WorkerState.Uninitialized)).toMap.toSeq:_*)

      //once workers are placed on their respective machines, they have to be initialized.
      allWorkers.foreach(x => x ! AckedWorkerInitialization)
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.become(initializing)
      unstashAll()
      sender ! AckWithInformation(metadata)
    case QueryState => sender ! ReportState(PrincipalState.Uninitialized)
    case msg =>
      //log.info("stashing: "+ msg)
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

          //once all workers have been created and put on machines, the
          // subsequent layers have to be linked.
          workerEdges.foreach(x => x.link())
          context.parent ! ReportState(PrincipalState.Ready)
          context.become(ready)
          unstashAll()
        }
      }
    case QueryState => sender ! ReportState(PrincipalState.Initializing)
    case msg =>
      //log.info("stashing: "+ msg)
      stash()
  }

}
