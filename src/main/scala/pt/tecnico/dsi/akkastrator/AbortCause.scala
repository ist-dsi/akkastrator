package pt.tecnico.dsi.akkastrator

trait AbortCause
case object TimedOut extends AbortCause
case object QuorumAlreadyAchieved extends AbortCause
case class InitializationError(message: String) extends AbortCause
case class ExceptionThrown(throwable: Throwable) extends AbortCause
case class OhSnap(message: String) extends AbortCause