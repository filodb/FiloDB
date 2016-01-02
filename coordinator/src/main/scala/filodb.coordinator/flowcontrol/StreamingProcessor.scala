package filodb.coordinator.reactive

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration


object StreamingProcessor {
  def defaultSuccess(int: Int) = {}

  def defaultFailure(int: Int, msg: FlowControlMessage) = {}

  def props[Input, Output](iterator: Iterator[Input],
                           chunkProcessor: Iterator[Input] => Output,
                           backPressureCheck: () => Boolean,
                           onSuccess: (Int) => Unit = defaultSuccess,
                           onFailure: (Int, FlowControlMessage) => Unit = defaultFailure,
                           chunkSize: Int = 1000,
                           maxRetries: Int = 10,
                           initialBackoff: Duration = Duration(.5, TimeUnit.SECONDS)): Props =
    Props(classOf[StreamingProcessor[Input, Output]],
      iterator, chunkProcessor,
      backPressureCheck, onSuccess, onFailure,
      chunkSize, maxRetries, initialBackoff)

  def start(actorRef: ActorRef) = {
    actorRef ! StartFlow
  }
}

class StreamingProcessor[Input, Output](iterator: Iterator[Input],
                                        chunkProcessor: Iterator[Input] => Output,
                                        backPressureCheck: () => Boolean,
                                        onSuccess: (Int) => Unit,
                                        onFailure: (Int, FlowControlMessage) => Unit,
                                        chunkSize: Int,
                                        maxRetries: Int,
                                        initialBackoff: Duration) extends Actor {

  def receive = {

    // start flow
    case StartFlow =>
      if (iterator.hasNext) {
        self ! Acknowledged(0)
      } else {
        // data stream is empty.
        self ! StopFlow
      }

    // continue processing
    case Acknowledged(size) if iterator.hasNext =>
      totalProcessed = totalProcessed + size
      if (retries < maxRetries) {
        val size = processConditionally
        if (size > 0) {
          // we are back processing again. So reset max retries
          retries = maxRetries
          self ! Acknowledged(size)
        } else {
          // back off exponentially until max retries
          Backoff(initialBackoff * Math.pow(2d, retries))
        }
      } else {
        self ! MaxRetriesExceeded(maxRetries)
      }

    // all chunks were sent. stop.
    case Acknowledged(size) =>
      totalProcessed = totalProcessed + size
      // we succeeded
      onSuccess(totalProcessed)
      self ! StopFlow

    // back off for a while
    case Backoff(forTime) =>
      implicit val context = ExecutionContext.global
      future {
        // sleep for back off duration
        Thread.sleep(forTime.toMillis)
      }(context) onSuccess {
        // and then start the flow again
        case _ => self ! StartFlow
      }

    // ah oh - game over
    case MaxRetriesExceeded(someRetries) =>
      // we failed. Call failure condition
      onFailure(totalProcessed, MaxRetriesExceeded(someRetries))
      context.stop(self)


    //  stop flow
    case StopFlow =>
      context.stop(self)

    // unhandled
    case x => unhandled(x)
  }

  var retries = 0
  var totalProcessed = 0
  var currentBackoff = initialBackoff

  def processConditionally: Int = {
    if (backPressureCheck()) {
      val buffer = ArrayBuffer[Input]()
      while (iterator.hasNext) {
        buffer += iterator.next
      }
      val size = buffer.size
      chunkProcessor(buffer.iterator)
      size
    } else {
      retries = retries - 1
      0
    }

  }
}
