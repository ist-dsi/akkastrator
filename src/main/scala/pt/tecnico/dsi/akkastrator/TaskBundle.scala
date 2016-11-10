package pt.tecnico.dsi.akkastrator

import scala.language.existentials
import scala.reflect.classTag

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskBundle.InnerOrchestrator
import shapeless.HNil

object TaskBundle {
  object InnerOrchestrator {
    def props[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], task: FullTask[_, _]): Props = {
      Props(classOf[InnerOrchestrator[R]], tasksCreator, task.orchestrator.persistenceId)
    }
  }
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]],
                             outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
    
    //Create the tasks and add them to this orchestrator
    tasksCreator(this)
  
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      //We know the cast will succeed because every task is a Task[R].
      val results = tasks.map(_.result.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
    
    //The default implementation of onAbort in AbstractOrchestrator is sufficient to handle the case when a task aborts.
  }
}

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Task return type must be the same
  *
  * @param tasksCreator
  * @tparam R the type the AbstractOrchestrator created in Props must have as its type parameter.
  */
class TaskBundle[R](task: FullTask[_, _])(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](InnerOrchestrator.props(tasksCreator, task), task)//(classTag[InnerOrchestrator[R]])