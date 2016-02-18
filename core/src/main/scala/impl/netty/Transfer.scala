package silt
package impl
package netty

import scala.pickling._
import scala.pickling.Defaults._

import scala.collection._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }

import _root_.io.netty.channel.Channel

trait Transfer {

  self: Correlation =>

  // XXX change return type to `Unit` but `sync`?
  def tell[M <: silt.Message: Pickler](via: Channel, message: M): Future[Channel] = {
    val promise = Promise[Channel]
    via.writeAndFlush(message) onComplete {
      case Success(c) => promise.success(c)
      case Failure(e) => promise.failure(e)
    }
    promise.future
  }

  def ask[R <: silt.RSVP: Pickler](via: Channel, request: R): Future[silt.Response] = {
    val promise = Promise[silt.Response]
    promiseOf += (request.id -> promise)
    via.writeAndFlush(request)
    promise.future
  }

}

// vim: set tw=120 ft=scala:
