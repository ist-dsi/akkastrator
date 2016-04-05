package pt.tecnico.dsi.akkastrator

import pt.tecnico.dsi.akkastrator.Message.MessageId

//Request
case class Status(id: MessageId)

object Task {
  sealed trait Status {
    val retryIteration: Int
  }
  case object Unstarted extends Status {
    val retryIteration = 0
  }
  case class Waiting(messageId: MessageId, retryIteration: Int = 0) extends Status
  case class WaitingToRetry(retryIteration: Int) extends Status
  case class Finished(messageId: MessageId, retryIteration: Int = 0) extends Status
}
//TODO: Should we state the dependencies between tasks?
case class Task(description: String, status: Task.Status)

//Response
case class StatusResponse(tasks: Seq[Task], id: MessageId)