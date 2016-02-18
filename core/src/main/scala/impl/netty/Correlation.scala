package silt
package impl
package netty

import scala.collection._
import scala.concurrent.Promise

trait Correlation {

  def promiseOf: mutable.Map[MsgId, Promise[Response]]

}

// vim: set tw=120 ft=scala:
