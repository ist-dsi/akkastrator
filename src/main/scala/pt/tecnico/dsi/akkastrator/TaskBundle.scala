package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskBundle.InnerOrchestrator

import scala.reflect.classTag

object TaskBundle {
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] ⇒ Seq[Task[R]], outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
    
    //Create the tasks and add them to this orchestrator
    tasksCreator(this)
  
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      //We just return the result of finished tasks.
      //We know the cast will succeed because every task is a Task[R].
      val results = tasks.flatMap(_.result).map(_.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
  }
  
  /**
    * Allows for the syntax:
    * {{{
    *  TaskBundle("bundle-description", dependencies = Set(...), someOtherTask.result.get) { s =>
    *    new Task(s"description-$$s", dependencies = Set(...))(_) {
    *      val destination: ActorPath = _//Something that might depend on s
    *      def createMessage(id: Long): Any = ???//Something that might depend on s
    *      def behavior: Receive = ???//Something that might depend on s
    *    }
    *  }
    * }}}
    */
  def apply[I, R](description: String, dependencies: Set[Task[_]] = Set.empty, collection: => Seq[I])
                 (taskCreator: I => AbstractOrchestrator[_] => Task[R])
                 (implicit orchestrator: AbstractOrchestrator[_]): TaskBundle[R] = {
    new TaskBundle[R](description, dependencies)(o => collection.map(i ⇒ taskCreator(i)(o)))
  }
  
  /**
    * Allows:
    * {{{
    *  def doFoo(someArg: Int)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *  def doBar(someArg: String)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *
    *  TaskBundle("bundle-description", dependencies = Set(...))(
    *    doFoo(42)(_),
    *    doBar("a string")(_)
    *  )
    * }}}
    */
  def apply[R](description: String, dependencies: Set[Task[_]])
              (tasksCreator: (AbstractOrchestrator[_] => Task[R])*)
              (implicit orchestrator: AbstractOrchestrator[_]): TaskBundle[R] = {
    new TaskBundle[R](description, dependencies)(o => tasksCreator.map(_(o)))
  }
}

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Task return type must be the same
  */
class TaskBundle[R](description: String, dependencies: Set[Task[_]] = Set.empty)
                   (tasksCreator: AbstractOrchestrator[_] => Seq[Task[R]])
                   (implicit orchestrator: AbstractOrchestrator[_])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    Props(classOf[InnerOrchestrator[R]], tasksCreator, orchestrator.persistenceId),
    description,
    dependencies)(classTag[InnerOrchestrator[R]], orchestrator)