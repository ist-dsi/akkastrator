package pt.tecnico.dsi.akkastrator

//TODO: is using an ADT a better approach?
/*
sealed trait AbortCause
case object TimedOut extends AbortCause
case object QuorumAlreadyAchieved extends AbortCause
case object QuorumImpossibleToAchieve extends AbortCause
object ExceptionThrown {
  def apply(message: String) = ExceptionThrown(new Throwable(message))
}
case class ExceptionThrown(throwable: Throwable) extends Exception(throwable) with AbortCause
*/

case object TimedOut extends Exception
case object QuorumNotAchieved extends Exception
case object QuorumAlreadyAchieved extends Exception
case object QuorumImpossibleToAchieve extends Exception
case class InitializationError(message: String) extends Exception(message)