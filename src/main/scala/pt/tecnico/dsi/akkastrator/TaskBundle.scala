package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.DSL.{FullTask, TaskBuilder}
import pt.tecnico.dsi.akkastrator.Orchestrator._
import shapeless.HNil

class Bundle[R](taskBuilders: Iterable[TaskBuilder[R]], outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
  def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
  
  // Create the tasks and add them to this orchestrator
  val fulltasks: Iterable[FullTask[R, HNil]] = taskBuilders.zipWithIndex.map { case (task, i) =>
    FullTask(i.toString).createTask(_ => task)
  }
  
  override def onFinish(): Unit = {
    log.info(withLogPrefix("Finished!"))
    val results = fulltasks.flatMap(_.result)
    context.parent ! Finished(results, startId)
    context stop self
  }
  
  //The default implementation of onTaskAbort and onAbort is sufficient to handle the case when a task aborts.
}

/**
  * A task that creates a variable number of tasks and succeeds when all the created tasks finish successfully.
  * The return type of the tasks must be the same. Their messages and destinations are unrestrained.
  * 
  * This task is specially useful to create `n` tasks where `n` is computed from the result of another task.
  * 
  * @param task
  * @param taskBuilders
  * @tparam R the return type of the tasks created using `tasksCreator`. 
  */
class TaskBundle[R](task: FullTask[Seq[R], _])(taskBuilders: Iterable[TaskBuilder[R]]) extends TaskSpawnOrchestrator[Seq[R], Bundle[R]](task)(
  Props(classOf[Bundle[R]], taskBuilders, task.orchestrator.persistenceId)
)