package server

import com.typesafe.scalalogging.{ StrictLogging => Logging }

import silt.netty.{ Server => NettyServer }

object Server extends AnyRef with Logging {
  import logger._

  def main(args: Array[String]): Unit = {
    NettyServer(8090).run()
  }
}
