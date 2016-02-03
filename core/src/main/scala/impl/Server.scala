package silt
package impl

/** The internal API of a F-P server. */
private[impl] trait Server extends Runnable {

  /** Host this server is running at */
  def at: Host

  /** Start server */
  def start(): Unit

  /** Stop server */
  def stop(): Unit

  // Members declared in java.lang.Runnable 

  final override def run(): Unit = start()

  ///** A F-P receptor to process silo system messages.
  //  *
  //  * Note: Only silo systems running in server mode require a F-P receptor.
  //  * This even holds true in case of peer to peer scenario, where at least on
  //  * participant must run in server mode in order to host silos.
  //  */
  //protected def receptor: Receptor

}

// vim: set tw=120 ft=scala:
