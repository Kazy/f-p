package silt
package impl
package netty

import java.util.concurrent.{ BlockingQueue, CountDownLatch }

import com.typesafe.scalalogging.{ StrictLogging => Logging }

private[netty] class Receptor(mq: BlockingQueue[silt.Message]) extends AnyRef /* with Transfer */ with Runnable with Logging {

  import logger._

  private val latch = new CountDownLatch(1)

  def start(): Unit = {
    trace("Receptor started.")

    while (latch.getCount > 0) { mq.take() match {
      case _ => logger.debug("mqed")
    } }
  }

  def stop(): Unit = {
    trace("Receptor stop...")

    latch.countDown()

    trace("Receptor stop done.")
  }

  // Members declared in java.lang.Runnable 

  final override def run(): Unit = start()

}

import io.netty.channel.{ ChannelFutureListener, ChannelHandler, ChannelHandlerContext }
import io.netty.channel.SimpleChannelInboundHandler

/* Forward incoming messages from Netty's event loop to internal message queue processed by [[Receptor]].
 *
 * Be aware that due to the default constructor parameter of [[io.netty.channel.SimpleChannelInboundHandler]] all
 * messages will be automatically released by passing them to [[io.netty.util.ReferenceCountUtil#release(Object)]].
 */
@ChannelHandler.Sharable
private[netty] class Forwarder(mq: BlockingQueue[silt.Message]) extends SimpleChannelInboundHandler[silt.Message] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, msg: silt.Message): Unit = {
    mq add msg
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    //cause.printStackTrace()

    // XXX Don't just close the connection when an exception is raised.
    //     With the current setup of the channel pipeline, in order to respond with a raw text message --- recall only a
    //     silo sytem can create system messages due to `Id` --- additional encoder/ decoder are required.
    val msg = s"${cause.getClass().getSimpleName()}: ${cause.getMessage()}"
    logger.error(msg)
    ctx.close()

    //import ChannelFutureListener._
    //if (ctx.channel().isActive())
    //  ctx.writeAndFlush(msg).addListener(CLOSE)
    //else ()

  }

}

// vim: set tw=120 ft=scala:
