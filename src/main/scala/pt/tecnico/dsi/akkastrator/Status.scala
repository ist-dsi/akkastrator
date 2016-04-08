package pt.tecnico.dsi.akkastrator

import pt.tecnico.dsi.akkastrator.Message.MessageId

//Request
case class Status(id: MessageId)

object Task {
  sealed trait Status
  case object Unstarted extends Status
  case object Waiting extends Status
  case object WaitingToRetry extends Status
  case object Finished extends Status
}
//TODO: Should we state the dependencies between tasks?
case class Task(description: String, status: Task.Status, numberOfRetries: Int)

//Response
case class StatusResponse(tasks: Seq[Task], id: MessageId)