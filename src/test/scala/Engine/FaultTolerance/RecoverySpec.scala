package Engine.FaultTolerance

import Clustering.SingleNodeListener
import Engine.Architecture.Controller.{Controller, ControllerState}
import Engine.Common.AmberMessage.ControlMessage.{Pause, RecoverCurrentStage, Resume, Start, StopCurrentStage}
import Engine.Common.AmberMessage.ControllerMessage.{AckedControllerInitialization, ReportState}
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.event.LoggingAdapter
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

class RecoverySpec
  extends TestKit(ActorSystem("RecoverySpec"))
with ImplicitSender
with FlatSpecLike
with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll:Unit = {
    system.actorOf(Props[SingleNodeListener],"cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val logicalPlan1 =
    """{
      |"operators":[
      |{"tableName":"D:\\large_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
      |{"attributeName":0,"keyword":"Asia","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan","destination":"KeywordSearch"},
      |{"origin":"KeywordSearch","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin

  private val logicalPlan3 =
    """{
      |"operators":[
      |{"tableName":"D:\\test.txt","operatorID":"Scan1","operatorType":"LocalScanSource","delimiter":"|"},
      |{"tableName":"D:\\test.txt","operatorID":"Scan2","operatorType":"LocalScanSource","delimiter":"|"},
      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
      |{"operatorID":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
      |{"operatorID":"GroupBy1","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
      |{"operatorID":"GroupBy2","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan1","destination":"KeywordSearch"},
      |{"origin":"Scan2","destination":"Join"},
      |{"origin":"KeywordSearch","destination":"Join"},
      |{"origin":"Join","destination":"GroupBy1"},
      |{"origin":"GroupBy1","destination":"GroupBy2"},
      |{"origin":"GroupBy2","destination":"Sink"}]
      |}""".stripMargin

  "A controller" should "stop execute the workflow1 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    Thread.sleep(100)
    controller ! StopCurrentStage
    parent.expectNoMessage(1.minute)
    parent.ref ! PoisonPill
  }



  "A controller" should "stop and restart execute the workflow1 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    Thread.sleep(100)
    controller ! StopCurrentStage
    parent.expectNoMessage(5.seconds)
    controller ! RecoverCurrentStage
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "A controller" should "stop and restart execute the workflow3 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan3))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    Thread.sleep(10)
    controller ! StopCurrentStage
    parent.expectNoMessage(5.seconds)
    controller ! RecoverCurrentStage
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "A controller" should "pause, stop and restart, then pause the execution of the workflow3" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan3))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    Thread.sleep(100)
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! StopCurrentStage
    parent.expectNoMessage(5.seconds)
    controller ! RecoverCurrentStage
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(5.minutes, ReportState(ControllerState.Running))
    parent.expectMsg(5.minutes, ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(5.minutes, ReportState(ControllerState.Resuming))
    parent.expectMsg(5.minutes, ReportState(ControllerState.Running))
    parent.expectMsg(5.minutes, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }



}
