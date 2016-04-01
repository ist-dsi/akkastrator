package pt.ulisboa.tecnico.dsi.akkastrator

import Message.{Message, MessageId}

sealed trait StatusRequest { self: Message =>
}

case class StatusById(messageId: MessageId, id: MessageId) extends StatusRequest
//This type of status query will only work if the equals method of Message ignores the id field.
case class StatusByMessage(message: Message, id: MessageId) extends StatusRequest