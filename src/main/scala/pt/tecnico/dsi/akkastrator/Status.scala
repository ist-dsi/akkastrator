package pt.tecnico.dsi.akkastrator

case object Status

case class TaskStatus(index: Int, description: String, status: Task.Status, dependencies: Set[Int])
case class StatusResponse(tasks: Seq[TaskStatus])