package pt.tecnico.dsi.akkastrator

sealed trait TaskState
case object Unstarted extends TaskState
case class Waiting(expectedDeliveryId: DeliveryId) extends TaskState
case class Finished[R](result: R) extends TaskState
case object TerminatedEarly extends TaskState