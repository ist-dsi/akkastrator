package pt.tecnico.dsi.akkastrator

import scala.collection.mutable

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import shapeless.HNil

/** Signals that every task in the Quorum has finished but a quorum has not achieved. */
case object QuorumNotAchieved extends Exception
/** Signals that the quorum is impossible to achieve since enough tasks have aborted that prevent the orchestrator
  * from achieving enough votes to satisfy the minimumVotes function. */
case object QuorumImpossibleToAchieve extends Exception

/** The quorum has already achieved however some tasks were still waiting. In this case the waiting tasks
  * are aborted with this cause.*/
case object QuorumAlreadyAchieved extends Exception

class Quorum[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]], minimumVotes: MinimumVotes,
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
      new IllegalArgumentException(s"""TasksCreator must generate tasks with distinct destinations.
                                      |Tasks "${t1.task.description}" and "${t2.task.description}" have the same destination:
                                      |\t${t1.destination}""".stripMargin)
    case Seq(t1, t2) if t1.createMessage(1L) != t2.createMessage(1L) => //TODO: This equality check might very easily fail
      new IllegalArgumentException(s"""TasksCreator must generate tasks with the same message.
                                      |Tasks "${t1.task.description}" and "${t2.task.description}" generate different messages:
                                      |\t${t1.createMessage(1L)}
                                      |\t${t2.createMessage(1L)}""".stripMargin)
  } foreach { cause =>
    onAbort(Aborted(cause, startId))
  }
  
  val votesToAchieveQuorum: Int = minimumVotes(tasks.length)
  
  protected val resultsCount = mutable.Map.empty[R, Int]
  protected var winningResult: R = _ // The result which has most votes so far
  protected var winningResultCount: Int = 0 // The number of votes of the winning result
  
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
    
    if (winningResultCount >= votesToAchieveQuorum && waitingTasks.nonEmpty) {
      // We achieved the quorum but we still have waiting tasks.
      // So we abort them and then manually invoke `onFinish` since it won't be automatically called.
      abortWaitingTasks(cause = QuorumAlreadyAchieved)(
        afterAllAborts = orchestrator.onFinish()
      )
      // Review: instead of aborting the waiting tasks we could have keep them running. And allow the users to
      // choose which way to go via a flag. However the later case complicates how a crash is handled/recovered.
    }
    // If the quorum has achieved in the last waiting task, aka waitingTasks.isEmpty then
    // `onFinish` will be called automatically.
    
    // In either case it is always the onFinish that returns the Finished result.
  }
  
  override def onFinish(): Unit = {
    log.info(s"${self.path.name} Finished!")
    
    if (winningResultCount >= votesToAchieveQuorum) {
      context.parent ! Finished(winningResult, startId)
    } else {
      // Every task has finished but we haven't achieved a quorum
      context.parent ! Aborted(QuorumNotAchieved, startId)
    }
    
    context stop self
  }
  
  // Tolerance = how many votes the quorum can afford to not obtain/lose (due to an aborted task) such that
  // its not necessary to terminate the quorum.
  // For example: if the quorum has 5 tasks and minimumVotes = Majority then at most 2 tasks can abort/not answer
  // if 3 tasks abort then we need to abort the orchestrator.
  protected var tolerance: Int = tasks.length - votesToAchieveQuorum
  
  override def onTaskAbort(instigator: FullTask[_, _], message: Any, cause: Exception): Unit = {
    tolerance -= 1
    if (tolerance < 0) {
      // We abort every waiting task to ensure that if this orchestrator crashes and is restarted
      // then it will correctly be restored to the correct state.
      abortWaitingTasks(QuorumImpossibleToAchieve)(
        afterAllAborts = super.onAbort(Aborted(QuorumImpossibleToAchieve, startId))
      )
    }
  }
  
  /** Aborts all tasks that are still waiting, but does not cause the orchestrator to abort. */
  private def abortWaitingTasks(cause: Exception)(afterAllAborts: => Unit): Unit = {
    waitingTasks.values.foreach { task =>
      // receivedMessage is set to None to make it more obvious that we did not receive a message for this task
      // task is waiting which ensures it has a expectedId aka .get will never throw
      task.innerAbort(receivedMessage = None, id = task.expectedId.get.self, cause) {
        if (waitingTasks.isEmpty) {
          afterAllAborts
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
  extends TaskSpawnOrchestrator[R, Quorum[R]](task)(
    Props(classOf[Quorum[R]], tasksCreator, minimumVotes, task.orchestrator.persistenceId)
  )