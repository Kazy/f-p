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
import _root_.io.netty.buffer.ByteBuf
import _root_.io.netty.bootstrap.Bootstrap
import _root_.io.netty.channel.{ ChannelInitializer, ChannelOption }
import _root_.io.netty.channel.nio.NioEventLoopGroup
import _root_.io.netty.channel.socket.SocketChannel
import _root_.io.netty.channel.socket.nio.NioSocketChannel
import _root_.io.netty.handler.logging.{ LogLevel, LoggingHandler => Logger }

trait Transfer {

  def statusOf: mutable.Map[Host, Status]

  def promiseOf: mutable.Map[Id, Promise[Response]]

  def connect(to: Host): Future[Channel] = statusOf.get(to) match {
    case None => channel(to) map { status =>
      statusOf += (to -> status)
      status.channel
    }
    case Some(Disconnected) =>
      statusOf -= to
      connect(to)
    case Some(Connected(channel, _)) => Future.successful(channel)
  }

  def channel(to: Host): Future[Connected] = {
    val wrkr = new NioEventLoopGroup
    try {
      val b = new Bootstrap
      b.group(wrkr)
       .channel(classOf[NioSocketChannel])
       .handler(new ChannelInitializer[SocketChannel] {
         override def initChannel(ch: SocketChannel): Unit = {
           val pipeline = ch.pipeline()
           pipeline.addLast(new Logger(LogLevel.TRACE))
           pipeline.addLast(encoder)
           pipeline.addLast(decoder)
           // XXX new ClientHandler { def systemImpl = self }
         }
       })
       .option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
       .connect(to.address, to.port).map(Connected(_, wrkr))
    } catch { case t: Throwable =>
      wrkr.shutdownGracefully()
      throw t
    }
  }

  def post[M <: silt.Message: Pickler](via: Channel, message: M): Future[Channel] = {
    val promise = Promise[Channel]

    via.writeAndFlush(message) onComplete {
      case Success(c) => promise.success(c)
      case Failure(e) => promise.failure(e)
    }

    promise.future
  }

  def send[R <: silt.RSVP: Pickler](via: Channel, request: R): Future[silt.Response] = {
    val promise = Promise[silt.Response]()

    promiseOf += (request.id -> promise)
    via.writeAndFlush(request)

    promise.future
  }

}

// vim: set tw=120 ft=scala:
