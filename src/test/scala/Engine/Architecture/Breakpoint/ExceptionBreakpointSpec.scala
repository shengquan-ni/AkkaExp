package Engine.Architecture.Breakpoint

import Clustering.SingleNodeListener
import Engine.Architecture.Breakpoint.GlobalBreakpoint.{ConditionalGlobalBreakpoint, CountGlobalBreakpoint}
import Engine.Architecture.Controller.{Controller, ControllerState}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.ControlMessage.{ModifyTuple, Resume, ResumeTuple, SkipTuple, Start}
import Engine.Common.AmberMessage.ControllerMessage.{AckedControllerInitialization, PassBreakpointTo, ReportGlobalBreakpointTriggered, ReportState}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import Engine.Common.AmberTuple.Tuple
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.event.LoggingAdapter
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

class ExceptionBreakpointSpec  extends TestKit(ActorSystem("PrincipalSpec"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val log:LoggingAdapter = system.log

  private val logicalPlan1 =
    """{
      |"operators":[
      |{"tableName":"D:\\fragmented_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
      |{"attributeName":0,"keyword":"Asia","operatorID":"KeywordSearch1","operatorType":"KeywordMatcher"},
      |{"attributeName":12,"keyword":"12","operatorID":"KeywordSearch2","operatorType":"KeywordMatcher"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan","destination":"KeywordSearch1"},
      |{"origin":"KeywordSearch1","destination":"KeywordSearch2"},
      |{"origin":"KeywordSearch2","destination":"Sink"}]
      |}""".stripMargin

  private val logicalPlan2 =
    """{
      |"operators":[
      |{"limit":10000,"delay":0,"operatorID":"Gen","operatorType":"Generate"},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Gen","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin

  val workflowTag = WorkflowTag("sample")
  var index=0
  val opTag: () => OperatorTag = ()=>{index+=1; OperatorTag(workflowTag,index.toString)}
  val layerTag: () => LayerTag = ()=>{index+=1; LayerTag(opTag(),index.toString)}
  val workerTag: () => WorkerTag = ()=>{index+=1; WorkerTag(layerTag(),index)}
  val linkTag: () => LinkTag = ()=>{LinkTag(layerTag(),layerTag())}

  def resultValidation(expectedTupleCount:Int,idleTime:Duration = 2.seconds): Unit ={
    var counter = 0
    var receivedEnd = false
    receiveWhile(5.minutes,idleTime){
      case DataMessage(seq,payload) => counter += payload.length
      case EndSending(seq) => receivedEnd = true
      case msg =>
    }
    assert(counter == expectedTupleCount)
    assert(receivedEnd)
  }

  override def beforeAll:Unit = {
    system.actorOf(Props[SingleNodeListener],"cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A workflow" should "be able to detect faulted tuples and trigger exception breakpoint in the workflow1, then skip them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportGlobalBreakpointTriggered(bp) =>
        for(i <- bp){
          log.info((if(i._1._2.isInput)"[IN]" else "[OUT]")+i._1._2.tuple+" ERRORS: ["+i._2.mkString(",")+"]")
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, SkipTuple(i._1._2),5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to detect faulted tuples and trigger exception breakpoint in the workflow1, then modify them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportGlobalBreakpointTriggered(bp) =>
        for(i <- bp){
          log.info((if(i._1._2.isInput)"[IN]" else "[OUT]")+i._1._2.tuple+" ERRORS: ["+i._2.mkString(",")+"]")
          val fixed = new FaultedTuple(Tuple("Asia","Rwanda","1","0","0","0","0","0","0","12","12","120","12"),i._1._2.id,i._1._2.isInput)
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, ModifyTuple(fixed),5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger conditional breakpoint in the workflow2, then resume them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("Gen", new ConditionalGlobalBreakpoint("ConditionalBreakpoint", x => x.getInt(0)%1000 == 0))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportGlobalBreakpointTriggered(bp) =>
        for(i <- bp){
          log.info((if(i._1._2.isInput)"[IN]" else "[OUT]")+i._1._2.tuple+" ERRORS: ["+i._2.mkString(",")+"]")
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, ResumeTuple(i._1._2),5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A workflow" should "be able to trigger conditional breakpoint in the workflow2, then skip them" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("Gen", new ConditionalGlobalBreakpoint("ConditionalBreakpoint", x => x.getInt(0)%1000 == 0))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportGlobalBreakpointTriggered(bp) =>
        for(i <- bp){
          log.info((if(i._1._2.isInput)"[IN]" else "[OUT]")+i._1._2.tuple+" ERRORS: ["+i._2.mkString(",")+"]")
          AdvancedMessageSending.blockingAskWithRetry(i._1._1, SkipTuple(i._1._2),5)
        }
        controller ! Resume
      case ReportState(ControllerState.Paused) =>
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

}
