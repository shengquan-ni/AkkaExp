package Engine.Architecture.Breakpoint

import Clustering.SingleNodeListener
import Engine.Architecture.Breakpoint.GlobalBreakpoint.CountGlobalBreakpoint
import Engine.Architecture.Controller.{Controller, ControllerState}
import Engine.Common.AmberMessage.ControlMessage.{Resume, Start}
import Engine.Common.AmberMessage.ControllerMessage.{AckedControllerInitialization, PassBreakpointTo, ReportGlobalBreakpointTriggered, ReportState}
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
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

  "A controller" should "be able to set and trigger count breakpoint in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportGlobalBreakpointTriggered =>
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }



}
