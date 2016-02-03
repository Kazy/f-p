package silt
package impl

import java.util.concurrent.CancellationException

import scala.concurrent.{ ExecutionContext, Promise, Future }
import scala.language.implicitConversions
import scala.util.Try

import io.netty.channel.{ Channel, ChannelFuture, ChannelFutureListener, ChannelHandlerContext }

package object netty {

  // Note: Decoder MUST NOT be a @Sharable handler!
  def decoder = new Decoder()
  val encoder = new Encoder()
  // XXX new LengthFieldBasedFrameDecoder ?
  // XXX new ChunkedWriteHandler() ?

  // Connection status

  import io.netty.channel.{ Channel, EventLoopGroup }

  sealed abstract class Status
  case class Connected(channel: Channel, worker: EventLoopGroup) extends Status
  case object Disconnected extends Status

  // Utilities

  implicit def nettyFutureToScalaFuture(future: ChannelFuture): Future[Channel] = {
    val p = Promise[Channel]()
    future.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture): Unit =
        p complete Try(
          if (future.isSuccess) future.channel
          else if (future.isCancelled) throw new CancellationException
          else throw future.cause
        )
    })
    p.future
  }

}

// vim: set tw=120 ft=scala:
