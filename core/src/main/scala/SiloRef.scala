package silt

import scala.concurrent.Future
import scala.pickling.{ Pickler, Unpickler }

/** Interface implemented by [[SiloSystem]], the only two places from which you can get fresh silos. */
trait SiloRefFactory {

  /** Uploads a silo to `host` with the initialization process of `clazz`.
    *
    * @param fac silo initialization logic
    * @param at target host of the the to be created silo
    *
    * @return a [[silt.SiloRef]], identifying the uploaded silo, as soon as the
    * initialization process has been completed.
    */
  def populate[T](at: Host, fac: SiloFactory[T])(implicit pickler: Pickler[Populate[T]]): Future[SiloRef[T]]

  /** Upload a silo to `host` with the initialization process defined by `data`.
    *
    * @tparam T
    *
    * @param fun silo initialization logic
    * @param at target host of the the to be created silo
    *
    * @return a [[silt.SiloRef]], identifying the uploaded silo, as soon as the
    * initialization process has been completed.
    */
  final def populate[T](at: Host, fun: () => Silo[T])(implicit pickler: Pickler[Populate[T]]): Future[SiloRef[T]] =
    populate(at, new SiloFactory[T] { override def data = fun().data })

}

final case class SiloRefId(id: RefId, at: Host)

/** Immutable and serializable handle to a silo.
  *
  * The referenced silo may or may not reside on the local host or inside the same silo system. A [[SiloRef]] can be
  * obtained from [[SiloRefFactory]], an interface which is implemented by [[SiloSystem]].
  *
  * @tparam T type of referenced silo's data
  */
trait SiloRef[T] {

  def id: SiloRefId

  final override def hashCode: Int = id.hashCode

  final override def equals(that: Any): Boolean = that match {
    case other: SiloRef[_] => id == other.id
    case _                 => false
  }

  // XXX def apply[S](fun: Spore[T, S])(implicit pickler: Pickler[Spore[T, S]], unpickler: Unpickler[Spore[T, S]]): SiloRef[S]

  // XXX def flatMap[S](fun: Spore[T, SiloRef[S]])(implicit pickler: Pickler[Spore[T, SiloRef[S]]], unpickler: Unpickler[Spore[T, SiloRef[S]]]): SiloRef[S]

  def send(): Future[T]

  //def pumpTo[V, R <: Traversable[V], P <: Spore2[W, Emitter[V], Unit]](destSilo: SiloRef[V, R])(fun: P)(implicit bf: BuilderFactory[V, R], pickler: Pickler[P], unpickler: Unpickler[P]): Unit = ???

}

abstract class SiloRefAdapter[T]() extends SiloRef[T] {

  def node(): graph.Node

  override def send(): Future[T] =
    ???

}

class Materialized[T](refId: RefId, at: Host) extends SiloRefAdapter[T] {

  override val id = SiloRefId(refId, at)

  override def node(): graph.Node = new graph.Materialized(id)

}
// vim: set tw=120 ft=scala:
