package pt.tecnico.dsi.akkastrator

import scala.collection.mutable

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskQuorum.InnerOrchestrator
import shapeless.HNil

/** A function that calculates how many votes are needed to achieve a quorum, given the number of destinations (nodes). */
trait MinimumVotes extends (Int => Int) {
  def apply(numberOfDestinations: Int): Int
}
/** A MinimumVotes function where a majority (51%) of votes are needed to achieve a quorum. */
object Majority extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = 1 + numberOfDestinations / 2
}
/**
  * A MinimumVotes function where at least `x` votes are needed to achieve a quorum.
  * If `x` is bigger than the number of destinations this function will behave like the `All` function.
  */
case class AtLeast(x: Int) extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = Math.min(x, numberOfDestinations)
}
/** A MinimumVotes function where every destination must answer in order for the quorum to be achieved. */
object All extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = numberOfDestinations
}

//TODO: create a flag that maintains the quorum running when the quorum as been achieved but not every task has answered
// In this case would we crash the orchestrator as soon as a task aborts?
// this might imply removing the spawner from the implementation of TaskSpawnOrchestrator

object TaskQuorum {
  object InnerOrchestrator {
    def props[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]],
                 minimumVotes: MinimumVotes, outerOrchestratorPersistenceId: String): Props = {
      Props(classOf[InnerOrchestrator[R]], tasksCreator, minimumVotes, outerOrchestratorPersistenceId)
    }
  }
  class InnerOrchestrator[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], minimumVotes: MinimumVotes,
                             outerOrchestratorPersistenceId: String) extends Orchestrator[R] {
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
      case Seq(t1, t2) if t1.createMessage(1L) != t2.createMessage(1L) => //TODO: This equality check might very easily fail
        (t2, InitializationError("TasksCreator must generate tasks with the same message."))
    } foreach { case (task, cause) =>
      // Saying a TaskAborted is an abuse of language, in fact it was the orchestrator that aborted
      context.parent ! TaskAborted(task.toTaskReport, cause, startId)
      context stop self
    }
    
    val votesToAchieveQuorum: Int = minimumVotes(tasks.length)
    
    protected val resultsCount = mutable.Map.empty[R, Int]
    protected var winningResult: R = _ // The result which has most votes so far
    protected var winningResultCount: Int = 0 // The number of votes of the winning result
    
    // Tolerance = how many votes the quorum can afford to not obtain/lose (due to an aborted task) such that
    // its not necessary to terminate the quorum inner orchestrator (this orchestrator)
    // For example: if the quorum has 5 tasks, and minimumVotes = Majority then at most 2 tasks can abort/not answer
    // if 3 tasks abort then we need to "abort" the orchestrator.
    protected var tolerance = tasks.length - votesToAchieveQuorum
  
    override def onTaskFinish(finishedTask: FullTask[_, _]): Unit = {
      val result = finishedTask.unsafeResult.asInstanceOf[R]
      val currentCount = resultsCount.getOrElse(result, 0)
      val newCount = currentCount + 1
      resultsCount(result) = newCount
    
      // Update the winning result
      if (newCount > winningResultCount) {
        winningResult = result
        winningResultCount = newCount
      }
    
      // Check whether we achieved the quorum
      if (winningResultCount >= votesToAchieveQuorum) {
        abortWaitingTasks(QuorumAlreadyAchieved)(
          afterAllAborts = orchestrator.onFinish()
        )
      }
    }
  
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      context.parent ! TasksFinished(winningResult, startId)
      context stop self
    }
  
    override def onAbort(instigator: FullTask[_, _], message: Any, cause: Exception, tasks: Map[Task.State, Seq[FullTask[_, _]]]): Unit = {
      tolerance -= 1
      if (tolerance < 0) {
        abortWaitingTasks(QuorumImpossibleToAchieve)(
          afterAllAborts = orchestrator.onAbort(instigator, message, cause, tasks)
        )
      }
    }
  
    /** Aborts all tasks that are still waiting, but does not cause the orchestrator to abort. */
    private def abortWaitingTasks(cause: Exception)(afterAllAborts: => Unit): Unit = {
      waitingTasks.foreach { case (_, task) =>
        //We know "get" will succeed because the task is waiting
        //receivedMessage is set to None to make it more obvious that we did not receive a message for this task
        task.innerAbort(receivedMessage = None, id = task.expectedDeliveryId.get.self, cause) {
          if (waitingTasks.isEmpty) {
            afterAllAborts
          }
        }
      }
    }
  }
}

/**
  * The quorum is obtained when X tasks finish and produce the same result, where X is calculated with the minimumVotes function.
  *
  * TaskQuorum:
  *   Variable number of tasks
  *   Task return type must be the same
  *   Different destinations
  *   Fixed message
  */
class TaskQuorum[R](task: FullTask[_, _], minimumVotes: MinimumVotes = Majority)(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[R, InnerOrchestrator[R]](task)(
    InnerOrchestrator.props(tasksCreator, minimumVotes, task.orchestrator.persistenceId)
  )