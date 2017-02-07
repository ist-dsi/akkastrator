package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import shapeless.HNil

class Bundle[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]],
                outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
  def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
  
  //Create the tasks and add them to this orchestrator
  tasksCreator(this)
  
  override def onFinish(): Unit = {
    log.info(s"${self.path.name} Finished!")
    //We know the cast will succeed because every task is a Task[R].
    val results: Seq[R] = tasks.flatMap(_.result.asInstanceOf[Option[R]])
    context.parent ! Finished(results, startId)
    context stop self
  }
  
  //The default implementation of onTaskAbort and onAbort is sufficient to handle the case when a task aborts.
}

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Task return type must be the same
  *   Unrestrained message and destination
  *
  * @param tasksCreator
  * @tparam R the type the AbstractOrchestrator created in Props must have as its type parameter.
  */
class TaskBundle[R](task: FullTask[_, _])(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[Seq[R], Bundle[R]](task)(
    Props(classOf[Bundle[R]], tasksCreator, task.orchestrator.persistenceId)
  )