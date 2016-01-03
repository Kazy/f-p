package silt
package impl

import scala.concurrent.Future

trait SiloSystem extends silt.SiloSystem with SiloSystemInternal {

  /** Return an implementation agnostic silo system running server mode.
    *
    * @param at [[Host network host]]
    */
  def withServer(at: Option[Host]): Future[silt.SiloSystem]

}

// vim: set tw=80 ft=scala: