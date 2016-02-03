package silt
package impl
package netty

import java.util.concurrent.{ BlockingQueue, CountDownLatch, LinkedBlockingQueue }

import scala.concurrent.{ ExecutionContext, Promise }
import ExecutionContext.Implicits.{ global => executor }

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ ChannelInitializer, ChannelOption }
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{ LogLevel, LoggingHandler => Logger }

import com.typesafe.scalalogging.{ StrictLogging => Logging }

private[netty] trait Server extends AnyRef with impl.Server with Logging {

  self: silt.SiloSystem =>

  import logger._

  /* Promise the server is up and running.  To fulfill this promise use `self` tied to [[silt.SiloSystem]]. */
  protected def started: Promise[silt.SiloSystem]

  /* Netty server constituents */
  private val server  = new ServerBootstrap
  private val bossGrp = new NioEventLoopGroup
  private val wrkrGrp = new NioEventLoopGroup

  /* Latch to prevent server termination before `stop` has been initiated by the surrounding silo system. */
  private val latch = new CountDownLatch(1)

  /* System message processing constituents
   * `mq`       : `forwarder --put--> mq <--pop-- receptor` 
   * `receptor` : Worker for all incoming messages from all channels.
   * `forwarder`: Forward system messages from Netty's event loop to `mq`
   */
  private val mq        = new LinkedBlockingQueue[silt.Message]()
  private val receptor  = new Receptor(mq)
  private val forwarder = new Forwarder(mq)

  /* Initialize a [[Netty http://goo.gl/0Z9pZM]]-based server.
   *
   * Note: [[NioEventLoopGroup]] is supposed to be used only for non-blocking
   * actions. Therefore we fork via [[EventExecutorGroup]] processing of F-P
   * system messages to the F-P receptor. 
   */
  trace("Server initializing...")
  server.group(bossGrp, wrkrGrp)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel): Unit = {
        val pipeline = ch.pipeline()
        pipeline.addLast(new Logger(LogLevel.TRACE))
        pipeline.addLast(encoder)
        pipeline.addLast(decoder)
        pipeline.addLast(forwarder)
      }
    })
    // XXX are those options necessary?
    //.option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128) 
    //.childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
  trace("Server initializing done.")

  // Members declared in silt.Server

  // Start and bind server to accept incoming connections at port `at.port`.
  override def start(): Unit =
    try {
      trace("Server start...")

      executor execute receptor
      server.bind(at.port).sync()
      started success self

      trace("Server start done.")
      info(s"Server listining at port ${at.port}.")

      (new Thread { override def run(): Unit = latch.await() }).start()
    } catch { case e: Throwable => started failure e }

  /** Stop server.
    *
    * In Nety 4.0, you can just call `shutdownGracefully` on the
    * `EventLoopGroup` that manages all your channels. Then all ''existing
    * channels will be closed automatically'' and reconnection attempts should
    * be rejected.
    */
  override def stop(): Unit = {
    trace("Server stop...")

    receptor.stop()
    wrkrGrp.shutdownGracefully()
    bossGrp.shutdownGracefully()
    latch.countDown()

    trace("Server stop done.")
  }

}

// vim: set tw=120 ft=scala:
