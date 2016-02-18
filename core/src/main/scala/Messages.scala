package silt

sealed trait Identifiable {

  val id: MsgId

}

sealed abstract class Message

sealed abstract class Request extends Message
case object Disconnect extends Request
case object Terminate  extends Request

sealed abstract class RSVP extends Request with Identifiable
case class  Populate[T](id: MsgId)(fac: SiloFactory[T]) extends RSVP 

sealed abstract class Response extends Message with Identifiable
case class Populated(id: MsgId)(val ref: RefId) extends Response

// vim: set tw=120 ft=scala:
