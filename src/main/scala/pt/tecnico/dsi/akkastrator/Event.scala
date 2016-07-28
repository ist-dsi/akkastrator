package pt.tecnico.dsi.akkastrator

sealed trait Event
case class MessageSent(taskIndex: Int) extends Event
case class MessageReceived(taskIndex: Int, response: Serializable) extends Event