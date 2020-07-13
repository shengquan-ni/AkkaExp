package Engine.Common

import Engine.Common.AmberMessage.ControlMessage.QueryState
import akka.actor.{ActorRef, Scheduler}
import akka.event.LoggingAdapter
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks
import scala.concurrent.duration._
//import com.twitter.util.{Await, Future, Duration}
//import com.twitter.bijection.Conversion.asMethod

import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

object AdvancedMessageSending {

  def nonBlockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, attempt: Int)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): scala.concurrent.Future[Any] = {
    val future = (receiver ? message) recover {
      case e: AskTimeoutException =>
        if (attempt > maxAttempts) log.error("failed to send message "+message+" to "+receiver)
        else nonBlockingAskWithRetry(receiver, message, maxAttempts, attempt + 1)
    }
    future
  }

  def nonBlockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, attempt: Int, callback: Any => Unit)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    (receiver ? message) onComplete {
      case Success(value) => callback(value)
      case Failure(exception) =>
        if (attempt > maxAttempts) log.error("failed to send message "+message+" to "+receiver)
        else nonBlockingAskWithRetry(receiver, message, maxAttempts, attempt + 1,callback)
    }
  }

//  def nonBlockingAskWithCondition(receiver: ActorRef, message: Any, cond:Any => Boolean, delay:FiniteDuration = 10.seconds)(implicit timeout:Timeout, ec:ExecutionContext, scheduler:Scheduler): Future[Any] ={
//    val future = (receiver ? message) recover {
//      case e: AskTimeoutException =>
//        (receiver ? QueryState) onComplete{
//          case Success(value) =>
//            if(cond(value)){
//              after(delay,scheduler){nonBlockingAskWithCondition(receiver,message,cond,delay)}
//            }
//          case Failure(_) =>
//            after(delay,scheduler){nonBlockingAskWithCondition(receiver,message,cond,delay)}
//        }
//    }
//    future
//  }

//  def blockingAskWithRetry(receivers: ArrayBuffer[ActorRef], message: Any, maxAttempts: Int)(implicit timeout: Timeout): Any = {
//    var futures: ArrayBuffer[com.twitter.util.Future[Any]] = new ArrayBuffer[com.twitter.util.Future[Any]]()
//    receivers.foreach(receiver => {
//      futures.append((receiver ? message).as[com.twitter.util.Future[Any]])
//    })
//    com.twitter.util.Await.all(futures: _*)
//
//  }

  def blockingAskWithRetry(receivers: Array[ActorRef], message: Any, maxAttempts: Int)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): ArrayBuffer[Any] = {
    var futures: ArrayBuffer[Future[Any]] = new ArrayBuffer[Future[Any]]()
    for(receiver <- receivers) {
      futures.append(receiver ? message)
    }

    // println(s"Futures size = ${futures.size}")

    var retArray = new ArrayBuffer[Any]()
    var i=0
    while(i < maxAttempts) {
      Try {
        for(future<-futures) {
          // note that we are not sending the message again
          val ret = scala.concurrent.Await.result(future,timeout.duration)
          retArray.append(ret)
        }
        Breaks.break()
      }
      retArray.clear()
      i += 1
    }
    retArray
  }

  //this is blocking the actor, be careful!
  def blockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Any ={
    var res:Any = null
    Breaks.breakable{
      var i = 0
      while(i < maxAttempts){
        Try{
          res = scala.concurrent.Await.result(receiver ? message,timeout.duration)
          Breaks.break()
        }
        i += 1
        println(s"***MESSAGE TIMEOUT***: Receiver didn't respond. Sending ${message.toString} for ${i}th time")
      }
      //log.error("failed to send message "+message+" to "+receiver)
    }
    res
  }

  //this is blocking the actor, be careful!
  def blockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, callback: Any => Unit)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Unit ={
    var res:Any = null
    Breaks.breakable{
      var i = 0
      while(i < maxAttempts){
        Try{
          res = scala.concurrent.Await.result(receiver ? message,timeout.duration)
          Breaks.break()
        }
        i += 1
      }
      log.error("failed to send message "+message+" to "+receiver)
    }
    callback(res)
  }

}
