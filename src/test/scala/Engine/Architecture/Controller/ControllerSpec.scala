package Engine.Architecture.Controller

import Clustering.SingleNodeListener
import Engine.Architecture.Breakpoint.GlobalBreakpoint.{ConditionalGlobalBreakpoint, CountGlobalBreakpoint}
import Engine.Common.AmberMessage.ControlMessage.{Ack, ModifyLogic, Pause, Resume, Start}
import Engine.Common.AmberMessage.ControllerMessage.{AckedControllerInitialization, PassBreakpointTo, ReportState}
import Engine.Common.AmberMessage.WorkerMessage.DataMessage
import Engine.Common.AmberTuple.Tuple
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

class ControllerSpec
  extends TestKit(ActorSystem("ControllerSpec"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll  {

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

  private val logicalPlan2 =
    """{
      |"operators":[
      |{"tableName":"D:\\large_input.csv","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":","},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin


  private val logicalPlan3 =
    """{
      |"operators":[
      |{"tableName":"D:\\test.txt","operatorID":"Scan","operatorType":"LocalScanSource","delimiter":"|"},
      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan","destination":"KeywordSearch"},
      |{"origin":"KeywordSearch","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin


  private val logicalPlan4 =
    """{
      |"operators":[
      |{"tableName":"D:\\test.txt","operatorID":"Scan1","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
      |{"tableName":"D:\\test.txt","operatorID":"Scan2","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
      |{"attributeName":15,"keyword":"package","operatorID":"KeywordSearch","operatorType":"KeywordMatcher"},
      |{"operatorID":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
      |{"operatorID":"Count","operatorType":"Aggregation"},
      |{"operatorID":"Sink","operatorType":"Sink"}],
      |"links":[
      |{"origin":"Scan1","destination":"KeywordSearch"},
      |{"origin":"KeywordSearch","destination":"Join"},
      |{"origin":"Scan2","destination":"Join"},
      |{"origin":"Join","destination":"Count"},
      |{"origin":"Count","destination":"Sink"}]
      |}""".stripMargin

  "A controller" should "be able to set and trigger count breakpoint in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch",new CountGlobalBreakpoint("break1",100000))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }



  "A controller" should "execute the workflow1 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "A controller" should "execute the workflow3 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan3))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }


  "A controller" should "execute the workflow2 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan2))
    controller ! AckedControllerInitialization
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }


  "A controller" should "be able to pause/resume the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "A controller" should "be able to modify the logic after pausing the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    controller ! Pause
    parent.expectMsg(ReportState(ControllerState.Pausing))
    parent.expectMsg(ReportState(ControllerState.Paused))
    controller ! ModifyLogic("{\"attributeName\":0,\"keyword\":\"Europe\",\"operatorID\":\"KeywordSearch\",\"operatorType\":\"KeywordMatcher\"}")
    parent.expectMsg(Ack)
    controller ! Resume
    parent.expectMsg(ReportState(ControllerState.Resuming))
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "A controller" should "be able to set and trigger conditional breakpoint in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch",new ConditionalGlobalBreakpoint("break2",x => x.getString(8).toInt == 9884))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A controller" should "be able to set and trigger count breakpoint on complete in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch",new CountGlobalBreakpoint("break1",146017))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }


  "A controller" should "be able to pause/resume with conditional breakpoint in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch",new ConditionalGlobalBreakpoint("break2",x => x.getString(8).toInt == 9884))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    val random = new Random()
    for(i <- 0 until 100){
      if(random.nextBoolean()) {
        controller ! Pause
      }else{
        controller ! Resume
      }
    }
    controller ! Resume
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }



  "A controller" should "be able to pause/resume with count breakpoint in the workflow1" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan1))
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds,ReportState(ControllerState.Ready))
    controller ! PassBreakpointTo("KeywordSearch",new CountGlobalBreakpoint("break1",100000))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    val random = new Random()
    for(i <- 0 until 100){
      if(random.nextBoolean()) {
        controller ! Pause
      }else{
        controller ! Resume
      }
    }
    controller ! Resume
    var isCompleted = false
    parent.receiveWhile(30.seconds,10.seconds){
      case ReportState(ControllerState.Paused) =>
        controller ! Resume
      case ReportState(ControllerState.Completed) =>
        isCompleted = true
      case _ =>
    }
    assert(isCompleted)
    parent.ref ! PoisonPill
  }

  "A controller" should "execute the workflow4 normally" in {
    val parent = TestProbe()
    val controller = parent.childActorOf(Controller.props(logicalPlan4))
    controller ! AckedControllerInitialization
    parent.expectMsg(ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }





}
