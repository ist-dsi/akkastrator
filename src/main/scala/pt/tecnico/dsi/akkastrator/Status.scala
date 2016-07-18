package pt.tecnico.dsi.akkastrator

case object Status

case class TaskDescription(index: Int, description: String, state: Task.State, dependencies: Set[Int])
case class StatusResponse(tasks: Seq[TaskDescription])