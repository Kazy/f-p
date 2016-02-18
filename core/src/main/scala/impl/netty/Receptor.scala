package silt
package impl
package netty

import java.util.concurrent.{ BlockingQueue, CountDownLatch }

import com.typesafe.scalalogging.{ StrictLogging => Logging }

private[netty] class Receptor(mq: BlockingQueue[silt.Message]) extends AnyRef /* with Transfer */ with Runnable with Logging {

  //self: Correlation =>

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

// vim: set tw=120 ft=scala:
