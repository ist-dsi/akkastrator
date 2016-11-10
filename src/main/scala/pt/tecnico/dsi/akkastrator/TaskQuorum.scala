package pt.tecnico.dsi.akkastrator

import scala.collection.mutable
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
      case Seq(t1, t2) if t1.createMessage(1L) != t2.createMessage(1L) =>  //TODO: This equality check might very easily fail
        (t2, InitializationError("TasksCreator must generate tasks with the same message."))
    } foreach { case (task, cause) =>
      // Saying a TaskAborted is an abuse of language, in fact it was the orchestrator that aborted
      context.parent ! TaskAborted(task.toTaskReport, cause, startId)
      context stop self
    }
    
    val votesToAchieveQuorum = minimumVotes(tasks.length)
    // Tolerance = how many votes the quorum can afford to not obtain/lose (due to an aborted) such that
    // its not necessary to terminate the quorum inner orchestrator (this orchestrator)
    // For example: if the quorum has 5 tasks, and minimumVotes = Majority then at most 2 tasks can abort/not answer
    // if 3 tasks abort then we need to finish the orchestrator, TODO: which reason to give for the abortion?
    val tolerance = tasks.length - votesToAchieveQuorum
    
    private[this] val resultsCount = mutable.Map.empty[R, Int]
    private[this] var winningResult: R = _ // The result which has most votes so far
    private[this] var winningResultCount: Int = 0 // The number of votes of the winning result
    
    override def onTaskFinish(finishedTask: FullTask[_, _]): Unit = {
      val result = finishedTask.result.asInstanceOf[R]
      val currentCount = resultsCount.getOrElse(result, 0)
      val newCount = currentCount + 1
      resultsCount(result) = newCount
      
      // Update the winning result
      if (newCount > winningResultCount) {
        winningResult = result
        winningResultCount = newCount
      }
      
      if (winningResultCount >= votesToAchieveQuorum) {
        // We already achieved a quorum. So now we want to abort all the tasks that are still waiting.
        // But not cause the orchestrator to abort.
        waitingTasks.foreach { case (_, task) =>
          log.info(task.withLogPrefix(s"Aborting due to $QuorumAlreadyAchieved"))
          //We know "get" will succeed because the task is waiting
          //receivedMessage is set to None to make it more obvious that we did not receive a message for this task
          task.persistAndConfirmDelivery(receivedMessage = None, id = task.expectedDeliveryId.get.self) {
            task.expectedDeliveryId = None
            task.state = Aborted(QuorumAlreadyAchieved)
            waitingTasks -= task.task.index
            context become orchestrator.computeCurrentBehavior()
  
            //TODO: does invoking onFinish inside the persistHandler cause any problem?
            if (waitingTasks.isEmpty) {
              orchestrator.onFinish()
            }
          }
        }
      }
    }
    
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      context.parent ! TasksFinished(winningResult, startId)
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
class TaskQuorum[R](task: FullTask[_, _], minimumVotes: MinimumVotes = Majority)(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    InnerOrchestrator.props(tasksCreator, minimumVotes, task), task
  )(classTag[InnerOrchestrator[R]])