package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskBundle.InnerOrchestrator

import scala.language.existentials
import scala.concurrent.duration.Duration
import scala.reflect.classTag

object TaskBundle {
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] ⇒ Seq[Task[R]], outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
    
    //Create the tasks and add them to this orchestrator
    tasksCreator(this)
  
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      //Using flatMap saves us from using .get. However we know that every task must have finished here.
      //We know the cast will succeed because every task is a Task[R].
      val results = tasks.flatMap(_.result).map(_.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
    
    //The default implementation of onAbort in AbstractOrchestrator is sufficient to handle the case when a task aborts.
  }
  
  /**
    * Allows for the syntax:
    * {{{
    * def doFoo(someArg: Int)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *
    * //Be sure that someOtherTask is either a direct dependency of the task bundle (as it is here) or its
    * //a transitive dependency. Otherwise someOtherTask.result.get will fail.
    * TaskBundle("bundle-description", dependencies = Set(someOtherTask))(someOtherTask.result.get) { i =>
    *   doFoo(i)(_)
    * }
    * }}}
    */
  def apply[I, R](description: String, dependencies: Set[Task[_]] = Set.empty, timeout: Duration = Duration.Inf)
                 (collection: => Seq[I])(taskCreator: I => AbstractOrchestrator[_] => Task[R])
                 (implicit orchestrator: AbstractOrchestrator[_]): TaskBundle[R] = {
    //It is VERY important that collection is a call-by-name argument.
    //This is what makes it possible to create a TaskBundle from the result of another task.
    //If the argument was not a call-by-name invoking .get on the task.result would always produce
    //the NoSuchElementException since at that time the task would still be Unstarted.
    new TaskBundle[R](description, dependencies, timeout)(o => collection.map(i ⇒ taskCreator(i)(o)))
  }
}

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Task return type must be the same
  */
class TaskBundle[R](description: String, dependencies: Set[Task[_]] = Set.empty, timeout: Duration = Duration.Inf)
                   (tasksCreator: AbstractOrchestrator[_] => Seq[Task[R]])
                   (implicit orchestrator: AbstractOrchestrator[_])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    description, dependencies, timeout,
    Props(classOf[InnerOrchestrator[R]], tasksCreator, orchestrator.persistenceId)
  )(classTag[InnerOrchestrator[R]], orchestrator)