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
    log.info(withLogPrefix("Finished!"))
    //We know the cast will succeed because every task is a Task[R].
    val results: Seq[R] = tasks.flatMap(_.result.asInstanceOf[Option[R]])
    context.parent ! Finished(results, startId)
    context stop self
  }
  
  //The default implementation of onTaskAbort and onAbort is sufficient to handle the case when a task aborts.
}

// tasksCreator should be of type `implicit AbstractOrchestrator[_] => Seq[FullTask[R, HNil]]`
// but unfortunately only Dotty has implicit function types)
/**
  * A task that creates a variable number of tasks and succeeds when all the created tasks finish successfully.
  * The return type of the tasks must be the same. Their messages and destinations are unrestrained.
  * 
  * This task is specially useful to create `n` tasks where `n` is computed from the result of another task.
  * 
  * @param task
  * @param tasksCreator
  * @tparam R the return type of the tasks created using `tasksCreator`. 
  */
class TaskBundle[R](task: FullTask[Seq[R], _])(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[Seq[R], Bundle[R]](task)(
    Props(classOf[Bundle[R]], tasksCreator, task.orchestrator.persistenceId)
  )