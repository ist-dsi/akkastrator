package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskQuorum.InnerOrchestrator

import scala.concurrent.duration.Duration
import scala.reflect.classTag

trait MinimumVotes extends (Int ⇒ Int) {
  def apply(numberOfDestinations: Int): Int
}
object Majority extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = Math.ceil(numberOfDestinations / 2.0).toInt
}
case class AtLeast(x: Int) extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = Math.min(x, numberOfDestinations)
}
object All extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = numberOfDestinations
}

object TaskQuorum {
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] => Seq[Task[R]], minimumVotes: MinimumVotes,
                             outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
  
    //Create the tasks and add them to this orchestrator
    tasksCreator(this)
    
    //Check that every created task:
    // · Has a distinct destination
    // · Generates the same message
    tasks.sliding(2).collectFirst {
      case Seq(t1, t2) if t1.destination == t2.destination =>
        (t2, InitializationError("TasksCreator must generate tasks with distinct destinations."))
      case Seq(t1, t2) if t1.createMessage(1L) != t2.createMessage(1L) =>
        (t2, InitializationError("TasksCreator must generate tasks with the same message."))
    } foreach { case (task, cause) =>
      context.parent ! TasksAborted(task.toTaskReport, cause, startId)
      context stop self
    }
    
    val votesToAchieveQuorum = minimumVotes(tasks.length)
    private var receivedVotes: Int = tasks.count(_.hasFinished)
    
    override def onTaskFinish(finishedTask: Task[_]): Unit = {
      receivedVotes += 1
      if (receivedVotes >= votesToAchieveQuorum) {
        //We already achieved a quorum. So now we want to abort all the tasks that are still waiting.
        //But not cause the orchestrator to abort.
        tasks.filter(_.isWaiting).foreach { task ⇒
          log.info(task.withLoggingPrefix(s"Aborting due to $QuorumAlreadyAchieved"))
          //We know "get" will succeed because the task is waiting
          //receivedMessage is set to None to make it more obvious that we did not receive a message for this task
          task.persistAndConfirmDelivery(receivedMessage = None, task.expectedDeliveryId.get.self) {
            task.expectedDeliveryId = None
            task.state = Task.Aborted(QuorumAlreadyAchieved)
          }
        }
        //Since we got the quorum we can finish the orchestrator
        onFinish()
      }
    }
    
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      //We just return the result of finished tasks.
      //We know the cast will succeed because every task is a Task[R].
      val results: Seq[R] = tasks.flatMap(_.result).map(_.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
  }
  
  /**
    * Allows for the syntax:
    * {{{
    *  def doFoo(someArg: Int)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *  TaskQuorum("quorum-description", dependencies = Set(...))(Majority, someOtherTask.result.get) { i =>
    *    doFoo(i)(_)
    *  }
    * }}}
    */
  def apply[I, R](description: String, dependencies: Set[Task[_]] = Set.empty, timeout: Duration = Duration.Inf, minimumVotes: MinimumVotes = Majority, collection: => Seq[I])
                 (taskCreator: I ⇒ AbstractOrchestrator[_] => Task[R])
                 (implicit orchestrator: AbstractOrchestrator[_]): TaskQuorum[R] = {
    new TaskQuorum[R](description, dependencies, timeout, minimumVotes)(o => collection.map(i => taskCreator(i)(o)))
  }
  
  /**
    * Allows:
    * {{{
    *  def doFoo(someArg: Int)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *  def doBar(someArg: String)(implicit orchestrator: AbstractOrchestrator[_]): Task[Int] = ???
    *
    *  TaskQuorum("quorum-description", dependencies = Set(...), timeout = Duration.Inf)(minimumVotes = Majority)(
    *    doFoo(42)(_),
    *    doBar("a string")(_)
    *  )
    * }}}
    */
  def apply[R](description: String, dependencies: Set[Task[_]], timeout: Duration, minimumVotes: MinimumVotes)
              (tasksCreator: (AbstractOrchestrator[_] => Task[R])*)
              (implicit orchestrator: AbstractOrchestrator[_]): TaskQuorum[R] = {
    new TaskQuorum[R](description, dependencies, timeout, minimumVotes)(o => tasksCreator.map(_(o)))
  }
}

/**
  * The quorum is obtained when X tasks finish, where X is calculated with the minimumVotes function.
  * This means that each destination might give different answers but as long as they produce finished tasks
  * these tasks will count towards the quorum.
  *
  * TaskQuorum:
  *   Variable number of tasks
  *   Different destinations
  *   Fixed message
  */
class TaskQuorum[R](description: String, dependencies: Set[Task[_]] = Set.empty, timeout: Duration = Duration.Inf, minimumVotes: MinimumVotes = Majority)
                   (tasksCreator: AbstractOrchestrator[_] => Seq[Task[R]])
                   (implicit orchestrator: AbstractOrchestrator[_])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    description, dependencies, timeout,
    Props(classOf[InnerOrchestrator[R]], tasksCreator, minimumVotes, orchestrator.persistenceId)
  )(classTag[InnerOrchestrator[R]], orchestrator)