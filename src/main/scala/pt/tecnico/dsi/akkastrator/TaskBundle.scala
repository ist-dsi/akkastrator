package pt.tecnico.dsi.akkastrator

import scala.reflect.classTag
import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskBundle.InnerOrchestrator

object TaskBundle {
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] ⇒ Seq[Task[R]], outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
    
    val createdTasks = tasksCreator(this)
    require(createdTasks.nonEmpty, "TasksCreator must create at least one task.")
    
    val firstDestination = tasks.head.destination
    require(tasks.forall(_.destination == firstDestination), "TasksCreator must generate tasks with the same destination.")
    require(tasks.forall(_.dependencies.isEmpty), "TasksCreator must generate tasks without dependencies.")
    
    //tasksCreator.map(creator ⇒ creator(this))
    
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      val results = tasks.map(_.result.get.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
  
    //If any task on the bundle causes an abort, the inner orchestrator will be aborted,
    //which in turn will cause the TaskSpawnOrchestrator to abort
  }
}

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Fixed destination
  *   Messages of the same type but with different values
  */
class TaskBundle[R](tasksCreator: AbstractOrchestrator[_] ⇒ Seq[Task[R]], description: String, dependencies: Set[Task[_]] = Set.empty)
                   (implicit orchestrator: AbstractOrchestrator[_])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    Props(classOf[InnerOrchestrator[R]], tasksCreator, orchestrator.persistenceId),
    description,
    dependencies)(classTag[InnerOrchestrator[R]], orchestrator)