package silt
package impl
package netty

import scala.collection._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }

import io.netty.channel.{ Channel, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler }
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ ChannelInitializer, ChannelOption }
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.logging.{ LogLevel, LoggingHandler => Logger }

import com.typesafe.scalalogging.{ StrictLogging => Logging }

trait Connection {

  self: Correlation =>

  def statusOf: mutable.Map[Host, Status]

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

  private def channel(to: Host): Future[Connected] = {
    val wrkr = new NioEventLoopGroup
    try {
      val b = new Bootstrap
      b.group(wrkr)
       .channel(classOf[NioSocketChannel])
       .handler(new ChannelInitializer[SocketChannel] {
         override def initChannel(ch: SocketChannel): Unit = {
           val pipeline = ch.pipeline()
           pipeline.addLast(new Logger(LogLevel.TRACE))
           pipeline.addLast(new Encoder())
           pipeline.addLast(new Decoder())
           pipeline.addLast(new ClientHandler()) 
         }
       })
       .option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
       .connect(to.address, to.port).map(Connected(_, wrkr))
    } catch { case t: Throwable =>
      wrkr.shutdownGracefully()
      throw t
    }
  }

  @ChannelHandler.Sharable
  private class ClientHandler() extends SimpleChannelInboundHandler[silt.Message] with Logging {
  
    import logger._
  
    override def channelRead0(ctx: ChannelHandlerContext, msg: silt.Message): Unit = {
      // response to request, so look up promise
      trace(s"Received message: $msg")
      msg match {
        case theMsg: Populated => promiseOf(theMsg.id).success(theMsg)
        case _                 => /* do nothing */
      }
    }
  
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      ctx.close()
    }
  
  }

}

// vim: set tw=120 ft=scala:
