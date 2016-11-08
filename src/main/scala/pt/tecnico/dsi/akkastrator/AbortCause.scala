package pt.tecnico.dsi.akkastrator

case object TimedOut extends Exception
case object QuorumAlreadyAchieved extends Exception
case class InitializationError(message: String) extends Exception(message)