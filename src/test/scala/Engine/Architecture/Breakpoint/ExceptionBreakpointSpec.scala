package Engine.Architecture.Breakpoint

import Clustering.SingleNodeListener
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
import akka.actor.{ActorSystem, Props}
import akka.event.LoggingAdapter
import akka.testkit.{ImplicitSender, TestKit}
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



}
