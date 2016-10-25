package pt.tecnico.dsi.akkastrator

/**
  * An immutable representation (a report) of a Task in a given moment of time.
  *
  * This is what is sent when someone queries the status of the orchestrator.
  */
case class TaskReport(description: String, dependencies: Seq[Int], state: Task.State)
//It does not include the destination for example because in a TaskProxy we don't know the destination
