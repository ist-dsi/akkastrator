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
                outerOrchestratorPersistenceId: String) extends DistinctIdsOrchestrator[R] {
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
    // Makes it more obvious when debugging the application
    log.error(withLogPrefix(cause.getMessage))
    onAbort(Aborted(cause, startId))
  }
  
  final val votesToAchieveQuorum: Int = minimumVotes(tasks.length)
  
  /**
    * Tolerance = how many votes the quorum can afford to not obtain/lose (due to an aborted task) such that
    * its not necessary to terminate the quorum.
    * For example: if the quorum has 5 tasks and minimumVotes = Majority then at most 2 tasks can abort/not answer
    * if 3 tasks abort then we need to abort the orchestrator.
    */
  final var tolerance: Int = tasks.length - votesToAchieveQuorum
  
  protected val resultsCount = mutable.Map.empty[R, Int]
  protected var winningResult: R = _ // The result which has most votes so far
  protected var winningResultCount: Int = 0 // The number of votes of the winning result
  
  log.info(withLogPrefix(s"${tasks.size} tasks, need $votesToAchieveQuorum votes to get a quorum, $tolerance tasks can fail."))
  
  override def onTaskFinish(task: FullTask[_, _]): Unit = {
    // Since the task has finished we know that there will be a result
    val result = task.unsafeResult.asInstanceOf[R]
    val currentCount = resultsCount.getOrElse(result, 0)
    val newCount = currentCount + 1
    resultsCount(result) = newCount
    
    // Update the winning result
    if (newCount > winningResultCount) {
      winningResult = result
      winningResultCount = newCount
    }
    
    log.debug(withLogPrefix(task.withLogPrefix(s"""Finished.
                                                  |\tWinning Result: $winningResult (with $winningResultCount votes)
                                                  |\tWaiting for ${waitingTasks.size} more votes, $tolerance task can fail""".stripMargin)))
    
    if (winningResultCount >= votesToAchieveQuorum) {
      log.info(withLogPrefix("Achieved quorum."))
      abortWaitingTasks(cause = QuorumAlreadyAchieved)(
        afterAllAborts = onFinish()
      )
    }
  }
  
  override def onFinish(): Unit = {
    log.info(withLogPrefix("Finished!"))
    
    if (winningResultCount >= votesToAchieveQuorum) {
      context.parent ! Finished(winningResult, startId)
    } else {
      // Every task has finished but we haven't achieved a quorum
      context.parent ! Aborted(QuorumNotAchieved, startId)
    }
    
    context stop self
  }
  
  override def onTaskAbort(task: FullTask[_, _], message: Any, cause: Exception): Unit = {
    tolerance -= 1
    
    log.debug(withLogPrefix(task.withLogPrefix(s"""Aborted.
                                                  |\tWinning Result: $winningResult, with $winningResultCount vote(s)
                                                  |\tWaiting for ${waitingTasks.size} more votes, $tolerance task can fail""".stripMargin)))
    
    if (tolerance < 0) {
      log.info(withLogPrefix("Tolerance surpassed."))
      abortWaitingTasks(QuorumImpossibleToAchieve)(
        afterAllAborts = onAbort(Aborted(QuorumImpossibleToAchieve, startId))
      )
    }
    
    if (tolerance == 0 && waitingTasks.isEmpty) {
      // This was the last task, however the tolerance has not surpassed.
      // So onFinish is invoked directly, it will terminate the Quorum with a QuorumNotAchieved.
      onFinish()
    }
  }
  
  /** Aborts all tasks that are still waiting, but does not cause the orchestrator to abort.
    * Then invokes the continuation `afterAllAborts`. If there are no waiting tasks simply invokes the continuation.*/
  final def abortWaitingTasks(cause: Exception)(afterAllAborts: => Unit): Unit = {
    if (waitingTasks.isEmpty) {
      afterAllAborts
    } else {
      // We abort every waiting task to ensure that if this orchestrator crashes and is restarted
      // then it will correctly be restored to the correct state. And no messages are sent to the destinations.
      waitingTasks.values.foreach { task =>
        // receivedMessage is set to None to make it more obvious that we did not receive a message for this task.
        // The task is waiting which ensures it has a expectedID aka .get will never throw
        task.innerAbort(receivedMessage = None, id = task.expectedID.get.self, cause) {
          if (waitingTasks.isEmpty) {
            afterAllAborts
          }
        }
      }
    }
  }
}

/**
  * The quorum is obtained when X tasks finish and produce the same result.
  * X is calculated with the minimumVotes function.
  *
  * TaskQuorum:
  *   Variable number of tasks
  *   Task return type must be the same
  *   Different destinations
  *   Fixed message
  */
class TaskQuorum[R](task: FullTask[_, _])(minimumVotes: MinimumVotes,
                                          tasksCreator: (AbstractOrchestrator[_]) => Seq[FullTask[R, HNil]])
  extends TaskSpawnOrchestrator[R, Quorum[R]](task)(
    Props(classOf[Quorum[R]], tasksCreator, minimumVotes, task.orchestrator.persistenceId)
  )