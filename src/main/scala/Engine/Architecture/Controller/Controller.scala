package Engine.Architecture.Controller


import Clustering.ClusterListener.GetAvailableNodeAddresses
import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Controller.ControllerEvent.WorkflowCompleted
import Engine.Architecture.DeploySemantics.DeployStrategy.OneOnEach
import Engine.Architecture.DeploySemantics.DeploymentFilter.FollowPrevious
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, GeneratorWorkerLayer, ProcessorWorkerLayer}
import Engine.FaultTolerance.Materializer.{HashBasedMaterializer, OutputMaterializer}
import Engine.FaultTolerance.Scanner.HDFSFolderScanTupleProducer
import Engine.Architecture.LinkSemantics.{FullRoundRobin, HashBasedShuffle, LocalPartialToOne, OperatorLink}
import Engine.Architecture.Principal.{Principal, PrincipalState}
import Engine.Common.AmberException.AmberException
import Engine.Common.AmberMessage.ControllerMessage._
import Engine.Common.AmberMessage.ControlMessage._
import Engine.Common.AmberMessage.PrincipalMessage
import Engine.Common.AmberMessage.PrincipalMessage.{AckedPrincipalInitialization, AssignBreakpoint, GetOutputLayer, ReportPrincipalPartialCompleted}
import Engine.Common.AmberMessage.StateMessage.EnforceStateCheck
import Engine.Common.AmberMessage.PrincipalMessage.ReportOutputResult
import Engine.Common.AmberTag.{AmberTag, LayerTag, LinkTag, OperatorTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{AdvancedMessageSending, AmberUtils, Constants, TupleProducer}
import Engine.Operators.SimpleCollection.SimpleSourceOperatorMetadata
import Engine.Operators.Count.CountMetadata
import Engine.Operators.Filter.{FilterMetadata, FilterType}
import Engine.Operators.GroupBy.{AggregationType, GroupByMetadata}
import Engine.Operators.HashJoin.HashJoinMetadata
import Engine.Operators.KeywordSearch.KeywordSearchMetadata
import Engine.Operators.OperatorMetadata
import Engine.Operators.Projection.ProjectionMetadata
import Engine.Operators.Scan.HDFSFileScan.{HDFSFileScanMetadata, HDFSFileScanTupleProducer}
import Engine.Operators.Scan.LocalFileScan.LocalFileScanMetadata
import Engine.Operators.Sink.SimpleSinkOperatorMetadata
import Engine.Operators.Sort.SortMetadata
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Address, Cancellable, Deploy, PoisonPill, Props, Stash}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import com.google.common.base.Stopwatch
import play.api.libs.json.{JsArray, JsValue, Json}
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


object Controller{

  implicit def ord: Ordering[DateTime] = Ordering.by(_.getMillis)

  def props(json:String, withCheckpoint:Boolean = false): Props = Props(fromJsonString(json,withCheckpoint))

  def props(tag: WorkflowTag, workflow: Workflow, withCheckpoint: Boolean, eventListener: ControllerEventListener, statusUpdateInterval: Long): Props =
    Props(new Controller(tag, workflow, withCheckpoint, Option.apply(eventListener), Option.apply(statusUpdateInterval)))

  private def fromJsonString(jsonString:String, withCheckpoint:Boolean):Controller ={
    val json: JsValue = Json.parse(jsonString)
    val tag:WorkflowTag = WorkflowTag("sample")
    val linkArray:JsArray = (json \ "links").as[JsArray]
    val links:Map[OperatorTag,Set[OperatorTag]] =
      linkArray.value.map(x => (OperatorTag(tag,x("origin").as[String]),OperatorTag(tag,x("destination").as[String])))
        .groupBy(_._1)
        .map { case (k,v) => (k,v.map(_._2).toSet)}
    val operatorArray:JsArray = (json \ "operators").as[JsArray]
    val operators:mutable.Map[OperatorTag,OperatorMetadata] = mutable.Map(operatorArray.value.map(x => (OperatorTag(tag,x("operatorID").as[String]),jsonToOperatorMetadata(tag,x))):_*)
    new Controller(tag,new Workflow(operators,links),withCheckpoint, Option.empty, Option.empty)

  }

  private def jsonToOperatorMetadata(workflowTag: WorkflowTag, json:JsValue): OperatorMetadata ={
    val id = json("operatorID").as[String]
    val tag = OperatorTag(workflowTag.workflow,id)
    json("operatorType").as[String] match{
      case "LocalScanSource" => new LocalFileScanMetadata(tag,Constants.defaultNumWorkers,json("tableName").as[String],json("delimiter").as[String].charAt(0),json("indicesToKeep").asOpt[Array[Int]].orNull,null)
      case "HDFSScanSource" => new HDFSFileScanMetadata(tag,Constants.defaultNumWorkers,json("host").as[String],json("tableName").as[String],json("delimiter").as[String].charAt(0),json("indicesToKeep").asOpt[Array[Int]].orNull,null)
      case "KeywordMatcher" => new KeywordSearchMetadata(tag,Constants.defaultNumWorkers,json("attributeName").as[Int],json("keyword").as[String])
      case "Aggregation" => new CountMetadata(tag,Constants.defaultNumWorkers)
      case "Filter" => new FilterMetadata[DateTime](tag,Constants.defaultNumWorkers,json("targetField").as[Int],FilterType.getType(json("filterType").as[String]),DateTime.parse(json("threshold").as[String]))
      case "Sink" => new SimpleSinkOperatorMetadata(tag)
      case "Generate" => new SimpleSourceOperatorMetadata(tag,Constants.defaultNumWorkers,json("limit").as[Int],json("delay").as[Int])
      case "HashJoin" => new HashJoinMetadata[String](tag,Constants.defaultNumWorkers,json("innerTableIndex").as[Int],json("outerTableIndex").as[Int])
      case "GroupBy" => new GroupByMetadata[String](tag,Constants.defaultNumWorkers,json("groupByField").as[Int],json("aggregateField").as[Int],AggregationType.valueOf(json("aggregationType").as[String]))
      case "Projection" => new ProjectionMetadata(tag,Constants.defaultNumWorkers,json("targetFields").as[Array[Int]])
      case "Sort" => new SortMetadata[String](tag,json("targetField").as[Int])
      case t => throw new NotImplementedError("Unknown operator type: "+ t)
    }
  }
}



class Controller
(
  val tag: WorkflowTag, val workflow: Workflow, val withCheckpoint: Boolean,
  val eventListener: Option[ControllerEventListener], val statisticsUpdateIntervalMs: Option[Long]
) extends Actor with ActorLogging with Stash {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout:Timeout = 5.seconds
  implicit val logAdapter: LoggingAdapter = log

  val principalBiMap:BiMap[OperatorTag,ActorRef] = HashBiMap.create[OperatorTag,ActorRef]()
  val principalStates = new mutable.AnyRefMap[ActorRef,PrincipalState.Value]
  val principalSinkResultMap = new mutable.HashMap[String, List[Tuple]]
  val edges = new mutable.AnyRefMap[LinkTag, OperatorLink]
  val frontier = new mutable.HashSet[OperatorTag]
  val stashedFrontier = new mutable.HashSet[OperatorTag]
  val stashedNodes = new mutable.HashSet[ActorRef]()
  val linksToIgnore = new mutable.HashSet[(OperatorTag,OperatorTag)]
  var periodicallyAskHandle:Cancellable = _
  var statusUpdateAskHandle: Cancellable = _
  var startDependencies = new mutable.HashMap[AmberTag,mutable.HashMap[AmberTag,mutable.HashSet[LayerTag]]]
  val timer = Stopwatch.createUnstarted();
  val pauseTimer = Stopwatch.createUnstarted();

  def allPrincipals: Iterable[ActorRef] = principalStates.keys
  def unCompletedPrincipals: Iterable[ActorRef] = principalStates.filter(x => x._2 != PrincipalState.Completed).keys
  def allUnCompletedPrincipalStates: Iterable[PrincipalState.Value] = principalStates.filter(x => x._2 != PrincipalState.Completed).values
  def availableNodes:Array[Address] = Await.result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses,5.seconds).asInstanceOf[Array[Address]]
  def getPrincipalNode(nodes:Array[Address]):Address = self.path.address//nodes(util.Random.nextInt(nodes.length))

  private def queryExecuteStatistics(): Unit = {

  }

  //if checkpoint activated:
  private def insertCheckpoint(from:OperatorMetadata,to:OperatorMetadata): Unit ={
    //insert checkpoint barrier between 2 operators and delete the link between them
    val topology = from.topology
    val hashFunc = to.getShuffleHashFunction(topology.layers.last.tag)
    val layerTag = LayerTag(from.tag,"checkpoint")
    val path:String = layerTag.getGlobalIdentity
    val numWorkers = topology.layers.last.numWorkers
    val scanGen:Int => TupleProducer = i => new HDFSFolderScanTupleProducer(Constants.remoteHDFSPath,path+"/"+i,'|',null)
    val lastLayer = topology.layers.last
    val materializerLayer = new ProcessorWorkerLayer(layerTag,i=>new HashBasedMaterializer(path,i,hashFunc,numWorkers),numWorkers,FollowPrevious(),OneOnEach())
    topology.layers :+= materializerLayer
    topology.links :+= new LocalPartialToOne(lastLayer,materializerLayer,Constants.defaultBatchSize)
    val scanLayer = new GeneratorWorkerLayer(LayerTag(to.tag,"from_checkpoint"),scanGen,topology.layers.last.numWorkers,FollowPrevious(),OneOnEach())
    val firstLayer = to.topology.layers.head
    to.topology.layers +:= scanLayer
    to.topology.links +:= new HashBasedShuffle(scanLayer,firstLayer,Constants.defaultBatchSize,hashFunc)

  }


  final def saveRemoveAskHandle(): Unit = {
    if (periodicallyAskHandle != null) {
      periodicallyAskHandle.cancel()
      periodicallyAskHandle = null
    }
  }

  override def receive: Receive = {
    case AckedControllerInitialization =>
      val nodes = availableNodes
      log.info("start initialization --------cluster have "+nodes.length+" nodes---------")
      for(k <- workflow.startOperators){
        val v = workflow.operators(k)
        val p = context.actorOf(Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),v.tag.operator)
        principalBiMap.put(k,p)
        principalStates(p) = PrincipalState.Uninitialized
      }
      workflow.startOperators.foreach(x => AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(x),AckedPrincipalInitialization(Array()),10,0, y => y match {
        case AckWithInformation(z) => workflow.operators(x) = z.asInstanceOf[OperatorMetadata]
        case other => throw new AmberException("principal didn't return updated metadata")
      } ))
      frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
    case ContinuedInitialization =>
      log.info("continue initialization")
      val nodes = availableNodes
      for(k <- frontier){
        val v = workflow.operators(k)
        val p = context.actorOf(Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),v.tag.operator)
        principalBiMap.put(k,p)
        principalStates(p) = PrincipalState.Uninitialized
      }
      workflow.startOperators.foreach(x => AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(x),AckedPrincipalInitialization(Array()),10,0, y => y match {
        case AckWithInformation(z) => workflow.operators(x) = z.asInstanceOf[OperatorMetadata]
        case other => throw new AmberException("principal didn't return updated metadata")
      } ))
      frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
    case PrincipalMessage.ReportState(state) =>
      assert(state == PrincipalState.Ready)
      principalStates(sender) = state
      if(principalStates.size == workflow.operators.size && principalStates.values.forall(_ == PrincipalState.Ready)){
        frontier.clear()
        if(stashedFrontier.nonEmpty) {
          log.info("partially initialized!")
          frontier ++= stashedFrontier
          stashedFrontier.clear()
        }else{
          log.info("fully initialized!")
//          for(i <- workflow.operators){
//            if(i._2.isInstanceOf[HDFSFileScanMetadata] && workflow.outLinks(i._1).head.operator.contains("Join")){
//              val node = principalBiMap.get(i._1)
//              AdvancedMessageSending.nonBlockingAskWithRetry(node,StashOutput,10,0)
//              stashedNodes.add(node)
//            }
//          }
        }
        context.parent ! ReportState(ControllerState.Ready)
        context.become(ready)
        if (this.statisticsUpdateIntervalMs.nonEmpty) {
//          statusUpdateAskHandle = context.system.scheduler.schedule(0.milliseconds,
//            FiniteDuration.apply(statisticsUpdateIntervalMs.get, MILLISECONDS), self, QueryStatistics)
        }
        unstashAll()
      }else{
        val next = frontier.filter(i => workflow.inLinks(i).forall(x => principalBiMap.containsKey(x) && principalStates(principalBiMap.get(x)) == PrincipalState.Ready))
        frontier --= next
        val prevInfo = next.flatMap(workflow.inLinks(_).map(x => (workflow.operators(x),Await.result(principalBiMap.get(x)?GetOutputLayer,5.seconds).asInstanceOf[ActorLayer]))).toArray
        val nodes = availableNodes
        val operatorsToWait = new ArrayBuffer[OperatorTag]
        for(k <- next){
          if(withCheckpoint){
            for(n <- workflow.outLinks(k)){
              if(workflow.operators(n).requiredShuffle){
                insertCheckpoint(workflow.operators(k),workflow.operators(n))
                operatorsToWait.append(k)
                linksToIgnore.add((k,n))
              }
            }
          }
          val v = workflow.operators(k)
          v.runtimeCheck(workflow) match {
            case Some(dependencies) => dependencies.foreach { x =>
              if (startDependencies.contains(x._1)) {
                startDependencies(x._1) ++= x._2
              } else {
                startDependencies.put(x._1, x._2)
              }
            }
            case None =>
          }
          val p = context.actorOf(Principal.props(v).withDeploy(Deploy(scope = RemoteScope(getPrincipalNode(nodes)))),v.tag.operator)
          principalBiMap.put(k,p)
          principalStates(p) = PrincipalState.Uninitialized
          AdvancedMessageSending.blockingAskWithRetry(principalBiMap.get(k),AckedPrincipalInitialization(prevInfo),10, y => y match {
            case AckWithInformation(z) => workflow.operators(k) = z.asInstanceOf[OperatorMetadata]
            case other => throw new AmberException("principal didn't return updated metadata")
          })
          for (from <- workflow.inLinks(k)) {
            if(!linksToIgnore.contains(from,k)){
              val edge = new OperatorLink((workflow.operators(from), principalBiMap.get(from)), (workflow.operators(k), principalBiMap.get(k)))
              edge.link()
              edges(edge.tag) = edge
            }
          }
        }
        next --= operatorsToWait
        frontier ++= next.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
        stashedFrontier ++= operatorsToWait.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
        frontier --= stashedFrontier
      }
    case msg => stash()
  }

  private[this] def ready:Receive ={
    case Start =>
      log.info("received start signal")
      timer.start()
      workflow.startOperators.foreach { x =>
        if(!startDependencies.contains(x))
          AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(x), Start, 10, 0)
      }
    case PrincipalMessage.ReportState(state) =>
      principalStates(sender) = state
      state match{
        case PrincipalState.Running =>
          log.info("workflow started!")
          context.parent ! ReportState(ControllerState.Running)
          context.become(running)
          unstashAll()
        case _ => throw new Exception("Invalid principal state received")
      }
    case PassBreakpointTo(id:String,breakpoint:GlobalBreakpoint) =>
      val opTag = OperatorTag(tag,id)
      if(principalBiMap.containsKey(opTag)){
        AdvancedMessageSending.blockingAskWithRetry(principalBiMap.get(opTag),AssignBreakpoint(breakpoint),3)
      }else {
        throw new AmberException("target operator not found")
      }
    case msg =>
      log.info("Stashing: "+msg)
      stash()
  }

  private[this] def running:Receive ={
    case PrincipalMessage.ReportState(state) =>
      principalStates(sender) = state
      state match{
        case PrincipalState.Completed =>
          log.info(sender+" completed")
          if(stashedNodes.contains(sender)){
            AdvancedMessageSending.nonBlockingAskWithRetry(sender,ReleaseOutput,10,0)
            stashedNodes.remove(sender)
          }
          if(principalStates.values.forall(_ == PrincipalState.Completed)) {
            timer.stop()
            log.info("workflow completed! Time Elapsed: "+timer.toString())
            timer.reset()
            saveRemoveAskHandle()
            if(frontier.isEmpty){
              context.parent ! ReportState(ControllerState.Completed)
              context.become(completed)
              // collect all output results back to controller
              val sinkPrincipals = this.workflow.endOperators.map(sinkOp => this.principalBiMap.get(sinkOp))
              for (sinkPrincipal <- sinkPrincipals) {
                sinkPrincipal ! CollectSinkResults
              }
              if (this.statusUpdateAskHandle != null) {
                this.statusUpdateAskHandle.cancel()
              }
//              self ! PoisonPill
            }else{
              context.become(receive)
              self ! ContinuedInitialization
            }
            unstashAll()
          }
        case PrincipalState.CollectingBreakpoints =>
        case _ => //skip others
      }
    case ReportGlobalBreakpointTriggered(bp) =>
      self ! Pause
      log.info(bp)
    case Pause =>
      pauseTimer.start()
      workflow.operators.foreach( x => principalBiMap.get(x._1) ! Pause)
      //workflow.startOperators.foreach(principalBiMap.get(_) ! Pause)
      //frontier ++= workflow.startOperators.flatMap(workflow.outLinks(_))
      log.info("received pause signal")
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.parent ! ReportState(ControllerState.Pausing)
      context.become(pausing)
      unstashAll()
    case ReportPrincipalPartialCompleted(from,layer) =>
      sender ! Ack
      for(i <- startDependencies.keys){
        if(startDependencies(i).contains(from) && startDependencies(i)(from).contains(layer)){
          startDependencies(i)(from) -= layer
          if(startDependencies(i)(from).isEmpty){
            startDependencies(i) -= from
            if(startDependencies(i).isEmpty){
              startDependencies -= i
              AdvancedMessageSending.nonBlockingAskWithRetry(principalBiMap.get(i), Start, 10, 0)
            }
          }
        }
      }
    case Resume =>
    case msg => stash()
  }

  private[this] def pausing:Receive ={
    case EnforceStateCheck =>
      frontier.flatMap(workflow.inLinks(_)).foreach(principalBiMap.get(_) ! QueryState)
    case PrincipalMessage.ReportState(state) =>
      if(state != PrincipalState.Paused && state != PrincipalState.Pausing && state != PrincipalState.Completed){
        sender ! Pause
      }else{
        principalStates(sender) = state
        if(principalStates.values.forall(_ == PrincipalState.Completed)){
          timer.stop()
          frontier.clear()
          log.info("workflow completed! Time Elapsed: "+timer.toString())
          timer.reset()
          saveRemoveAskHandle()
          if(frontier.isEmpty){
            context.parent ! ReportState(ControllerState.Completed)
            context.become(completed)
          }else{
            context.become(receive)
            self ! ContinuedInitialization
          }
          unstashAll()
        }else if(allUnCompletedPrincipalStates.forall(_ == PrincipalState.Paused)){
          pauseTimer.stop()
          frontier.clear()
          log.info("workflow paused! Time Elapsed: "+pauseTimer.toString())
          pauseTimer.reset()
          saveRemoveAskHandle()
          context.parent ! ReportState(ControllerState.Paused)
          context.become(paused)
          unstashAll()
        }else{
          val next = frontier.filter(i => workflow.inLinks(i).map(x => principalStates(principalBiMap.get(x))).forall(x => x == PrincipalState.Paused || x == PrincipalState.Completed))
          frontier --= next
          next.foreach(principalBiMap.get(_) ! Pause)
          frontier ++= next.filter(workflow.outLinks.contains).flatMap(workflow.outLinks(_))
        }
      }
    case ReportGlobalBreakpointTriggered(bp) => log.info(bp)
    case msg => stash()
  }

  private[this] def paused:Receive = {
    case Resume =>
      workflow.endOperators.foreach(principalBiMap.get(_) ! Resume)
      frontier ++= workflow.endOperators.flatMap(workflow.inLinks(_))
      log.info("received resume signal")
      saveRemoveAskHandle()
      periodicallyAskHandle = context.system.scheduler.schedule(30.seconds,30.seconds,self,EnforceStateCheck)
      context.parent ! ReportState(ControllerState.Resuming)
      context.become(resuming)
      unstashAll()
    case Pause =>
    case EnforceStateCheck =>
    case ModifyLogic(newLogic) =>
      // newLogic is something like {"operatorID":"Filter","operatorType":"Filter","targetField":2,"filterType":"Greater","threshold":"1991-01-01"}
      val json: JsValue = Json.parse(newLogic)
      val id = json("operatorID").as[String]
      val operatorTag = OperatorTag(tag.workflow,id)
      val principal: ActorRef = principalBiMap.get(operatorTag)
      AdvancedMessageSending.blockingAskWithRetry(principal, ModifyLogic(newLogic), 3)
      context.parent ! Ack
    case msg => stash()
  }

  private[this] def resuming: Receive = {
    case EnforceStateCheck =>
      frontier.flatMap(workflow.outLinks(_)).foreach(principalBiMap.get(_) ! QueryState)
    case PrincipalMessage.ReportState(state) =>
      if(state != PrincipalState.Resuming && state != PrincipalState.Running && state != PrincipalState.Ready){
        sender ! Resume
      }else{
        principalStates(sender) = state
        if(principalStates.values.forall(_ != PrincipalState.Paused)){
          frontier.clear()
          if(principalStates.values.exists(_ != PrincipalState.Ready)) {
            log.info("workflow resumed!")
            saveRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Running)
            context.become(running)
            unstashAll()
          } else{
            log.info("workflow ready!")
            saveRemoveAskHandle()
            context.parent ! ReportState(ControllerState.Ready)
            context.become(ready)
            unstashAll()
          }
        }else{
          val next = frontier.filter(i => !workflow.outLinks(i).map(x => principalStates(principalBiMap.get(x))).contains(PrincipalState.Paused))
          frontier --= next
          next.foreach(principalBiMap.get(_) ! Resume)
          frontier ++= next.filter(workflow.inLinks.contains).flatMap(workflow.inLinks(_))
        }
      }
    case msg => stash()
  }

  private[this] def completed:Receive = {
    case PrincipalMessage.ReportOutputResult(sinkResults) =>
      val operatorID = this.principalBiMap.inverse().get(sender()).operator
      this.principalSinkResultMap(operatorID) = sinkResults
      if (this.principalSinkResultMap.size == this.workflow.endOperators.size) {
        if (this.eventListener != null && this.eventListener.get.workflowCompletedListener != null) {
          this.eventListener.get.workflowCompletedListener.apply(WorkflowCompleted(this.principalSinkResultMap.toMap))
        }
        self ! PoisonPill
      }
    case msg =>
      log.info("received: {} after workflow completed!",msg)
      if(sender !=self && !principalStates.keySet.contains(sender)){
        sender ! ReportState(ControllerState.Completed)
      }
  }

}
