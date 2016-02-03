package silt
package impl
package netty

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future, Promise }
import ExecutionContext.Implicits.{ global => executor }
import scala.pickling._
import scala.pickling.Defaults._

import com.typesafe.scalalogging.{ StrictLogging => Logging }

/** A Netty-based implementation of a silo system. */
class SiloSystem extends AnyRef with impl.SiloSystem with Transfer with Logging {

  import logger._

  // Members declared in silt.impl.Transfer

  override val statusOf = new TrieMap[Host, Status]

  override val promiseOf = new TrieMap[Id, Promise[Response]]

  // Members declared in silt.SiloSystem

  override val name = (java.util.UUID.randomUUID()).toString

  override def terminate(): Future[Unit] = {
    val promise = Promise[Unit]

    info(s"Silo system `$name` terminating...")
    val to = statusOf collect { case (host, Connected(channel, worker)) =>
      trace(s"Closing connection to `$host`.")
      post(channel, Disconnect)
        .andThen { case _ => worker.shutdownGracefully() }
        .andThen { case _ => statusOf += (host -> Disconnected) }
    }

    // Close connections FROM other silo systems
    // val from = XXX

    // Terminate underlying server
    Future.sequence(to /*, from*/ ) onComplete { case _ =>
      promise success (this match { case server: Server => server.stop() case _ => () })
      info(s"Silo system `$name` terminating done.")
    }

    promise.future
  }

  // Members declared in silt.Internals

  override def initiate[R <: silt.RSVP: Pickler](at: Host)(request: Id => R): Future[silt.Response] =
    connect(at) flatMap { via => send(via, request(msgId.next)) }

  override def withServer(host: Host): Future[silt.SiloSystem] = {
    val promise = Promise[silt.SiloSystem]
    executor execute (new SiloSystem with Server {
      override val at = host
      override val name = host.toString
      override val started = promise
    })
    promise.future
  }

}

// vim: set tw=120 ft=scala:
