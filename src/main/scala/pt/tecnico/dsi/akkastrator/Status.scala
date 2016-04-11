package pt.tecnico.dsi.akkastrator

case class Status(id: Long)

case class TaskStatus(index: Int, description: String, status: Task.Status, dependencies: Set[Int])
case class StatusResponse(tasks: Seq[TaskStatus], id: Long)