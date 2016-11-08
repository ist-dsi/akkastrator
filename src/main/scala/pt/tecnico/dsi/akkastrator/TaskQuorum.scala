package pt.tecnico.dsi.akkastrator

import scala.reflect.classTag

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.{Aborted, Finished}
import pt.tecnico.dsi.akkastrator.TaskQuorum.InnerOrchestrator
import shapeless.HNil

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

//TODO: create a flag that maintains the quorum running when the quorum as been achieved but not every task has answered
// this might imply removing the spawner from the implementation of TaskSpawnOrchestrator


object TaskQuorum {
  object InnerOrchestrator {
    def props[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], minimumVotes: MinimumVotes, task: FullTask[_, _]): Props = {
      Props(classOf[InnerOrchestrator[R]], tasksCreator, minimumVotes, task.orchestrator.persistenceId)
    }
  }
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], minimumVotes: MinimumVotes,
                             outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
  
    // Create the tasks and add them to this orchestrator
    tasksCreator(this)
    
    // Check that every created task:
    // · Has a distinct destination
    // · Generates the same message
    // Since we required the created tasks to have no dependencies we know they were added to the waitingTasks list.
    waitingTasks.values.sliding(2).collectFirst {
      case Seq(t1, t2) if t1.destination == t2.destination =>
        (t2, InitializationError("TasksCreator must generate tasks with distinct destinations."))
      case Seq(t1, t2) if t1.createMessage(1L) != t2.createMessage(1L) =>
        // The check being performed is not fail proof because the user might have implemented a createMessage in task
        // which returns different messages according to the id.
        (t2, InitializationError("TasksCreator must generate tasks with the same message."))
    } foreach { case (task, cause) =>
      // Saying a TaskAborted is an abuse of language, in fact it was the orchestrator that aborted
      context.parent ! TaskAborted(task.toTaskReport, cause, startId)
      context stop self
    }
    
    val votesToAchieveQuorum = minimumVotes(tasks.length)
    private var receivedVotes: Int = finishedTasks
    
    override def onTaskFinish(finishedTask: FullTask[_, _]): Unit = {
      receivedVotes += 1
      if (receivedVotes >= votesToAchieveQuorum) {
        // We already achieved a quorum. So now we want to abort all the tasks that are still waiting.
        // But not cause the orchestrator to abort.
        waitingTasks.foreach { case (_, task) =>
          log.info(task.withLogPrefix(s"Aborting due to $QuorumAlreadyAchieved"))
          //We know "get" will succeed because the task is waiting
          //receivedMessage is set to None to make it more obvious that we did not receive a message for this task
          task.persistAndConfirmDelivery(receivedMessage = None, task.expectedDeliveryId.get.self) {
            task.expectedDeliveryId = None
            task.state = Aborted(QuorumAlreadyAchieved)
            waitingTasks -= task.task.index
            context become orchestrator.computeCurrentBehavior()
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
      val results: Seq[R] = tasks.map(_.state).collect {
        case Finished(result) => result.asInstanceOf[R]
      }
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
  
    //The default implementation of onAbort in AbstractOrchestrator is sufficient to handle the case when a task aborts.
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
class TaskQuorum[R](task: FullTask[_, _])(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], minimumVotes: MinimumVotes = Majority)
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    InnerOrchestrator.props(tasksCreator, minimumVotes, task), task
  )(classTag[InnerOrchestrator[R]])