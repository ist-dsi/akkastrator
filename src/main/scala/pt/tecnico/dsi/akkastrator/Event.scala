package pt.tecnico.dsi.akkastrator

object Event {
  // TODO: add ProtocolBuffers for these messages
  sealed trait Event
  // We do not persist the request because:
  //  1. The Task.createMessage is a function, and serializing functions is troublesome to say the least.
  //  2. We do not need to. When an orchestrator is recreated it also recreates its Tasks which means the
  //     request is available by invoking createMessage again.
  //  3. We are not adding to the event journal possibly sensitive information like hashed passwords.
  case class TaskStarted(taskIndex: Int) extends Event
  // We are however adding to the event journal the result. We can't escape it because we need it in order to invoke the task behavior.
  case class TaskFinished[R](taskIndex: Int, result: R) extends Event {
    // Hack to circumvent the type parameter R. I dont understand how this even works. If you do let me know.
    private[akkastrator] def finish(task: Task[_]): Unit = task.asInstanceOf[Task[R]].finish(result)
  }
  case class TaskAborted(taskIndex: Int, exception: Throwable) extends Event
}

