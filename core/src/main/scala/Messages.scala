package silt

sealed trait Identifiable {

  val id: Id

}

sealed abstract class Message

sealed abstract class Request extends Message
case object Disconnect extends Request
case object Terminate  extends Request

sealed abstract class RSVP extends Request with Identifiable
case class  Populate[T](id: Id, fac: SiloFactory[T]) extends RSVP 

sealed abstract class Response extends Message with Identifiable
case class Populated(id: Id, cor: Id) extends Response

// vim: set tw=120 ft=scala:
