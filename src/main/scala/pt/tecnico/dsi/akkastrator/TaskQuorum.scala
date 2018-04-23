package pt.tecnico.dsi.akkastrator

import scala.collection.mutable
import scala.util.Random

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.DSL.{FullTask, TaskBuilder}
import pt.tecnico.dsi.akkastrator.Orchestrator._

/** Signals that every task in the Quorum has finished but a quorum has not achieved. */
case object QuorumNotAchieved extends Exception

/** Signals that the quorum is impossible to achieve since enough tasks have aborted that prevent the orchestrator
  * from achieving enough votes to satisfy the minimumVotes function. */
case object QuorumImpossibleToAchieve extends Exception

/** Exception used to abort waiting tasks when the quorum was already achieved. */
case object QuorumAlreadyAchieved extends Exception

class Quorum[R](taskBuilders: Iterable[TaskBuilder[R]], minimumVotes: MinimumVotes, outerOrchestratorPersistenceId: String) extends Orchestrator[R] {
  def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
  
  // Create the tasks and add them to this orchestrator
  for ((task, i) <- taskBuilders.zipWithIndex) {
    FullTask(i.toString).createTask(_ => task)
  }
  
  // Check that every created task:
  // · Has a distinct destination
  // · Generates the same message
  // Since the tasks have no dependencies we know they were added to the waitingTasks list.
  private val randomId = Random.nextLong
  waitingTasks.values.toStream
    .map(t => (t.task.description, t.destination, t.createMessage(randomId)))
    .sliding(2)
    .collectFirst {
      case Stream((descriptionA, destA, _), (descriptionB, destB, _)) if destA == destB =>
        new IllegalArgumentException( s"""Quorum tasks must have distinct destinations.
                                         |Tasks "$descriptionA" and "$descriptionB" have the same destination:
                                         |\t$destA""".stripMargin)
      case Seq((descriptionA, _, messageA), (descriptionB, _, messageB)) if messageA != messageB =>
        new IllegalArgumentException( s"""Quorum tasks must generate the same message.
                                         |Tasks "$descriptionA" and "$descriptionB" generate different messages:
                                         |\t$messageA
                                         |\t$messageB""".stripMargin)
    }
    .foreach { cause =>
      // Makes it more obvious when debugging the application
      log.error(withLogPrefix(cause.getMessage))
      onAbort(Aborted(cause, startId))
    }
  
  final val votesToAchieveQuorum: Int = minimumVotes(tasks.length)
  
  /**
    * Tolerance = how many votes the quorum can afford to not obtain/lose (due to an aborted task) such that
    * its not necessary to terminate the quorum.
    * For example: if the quorum has 5 tasks and minimumVotes = Majority = 3 then at most 2 tasks can abort/not answer
    * if 3 tasks abort then we need to abort the orchestrator.
    */
  final var tolerance: Int = tasks.length - votesToAchieveQuorum
  
  protected val resultsCount = mutable.Map.empty[R, Int]
  protected var winningResult: R = _ // The result which has most votes so far
  protected var winningResultCount: Int = 0 // The number of votes of the winning result
  
  override def onStart(startId: Long): Unit = {
    super.onStart(startId)
    log.info(withLogPrefix(s"${tasks.size} tasks, need $votesToAchieveQuorum votes to get a quorum, $tolerance tasks can fail."))
  }
  
  protected def pluralize(word: String, count: Int): String = s"$word${if (count > 1) "s" else ""}"
  
  protected def quorumStatus: String = s"""
                                          |\tWinning Result (with $winningResultCount ${pluralize("vote", winningResultCount)}: $winningResult
                                          |\tExpecting ${waitingTasks.size} more ${pluralize("vote", waitingTasks.size)}, $tolerance ${pluralize("tasks", tolerance)} can fail""".stripMargin
  
  override def onTaskFinish(task: FullTask[_, _]): Unit = {
    val result = task.unsafeResult.asInstanceOf[R] // unsafeResult will be safe because the task has finished
    val newCount = resultsCount.getOrElse(result, 0) + 1
    resultsCount(result) = newCount
    
    // Update the winning result
    if (newCount > winningResultCount) {
      winningResult = result
      winningResultCount = newCount
    }
    
    super.onTaskFinish(task)
    
    log.debug(withLogPrefix(quorumStatus))
    
    if (winningResultCount >= votesToAchieveQuorum) {
      log.info(withLogPrefix("Achieved quorum."))
      // We abort the waiting tasks to ensure that if this orchestrator crashes and is restarted, then it will
      // be restored to the correct state, and no messages are sent to the destinations.
      waitingTasks.values.foreach(_.abort(QuorumAlreadyAchieved))
      onFinish()
    }
  }
  
  override def onTaskAbort(task: FullTask[_, _], cause: Throwable): Unit = {
    if (winningResultCount >= votesToAchieveQuorum || tolerance < 0) {
      // task was aborted either because we already achieved quorum or because the tolerance was surpassed.
      // For these cases we dont run the tolerance logic.
    } else {
      tolerance -= 1
      // We cant do `super.onTaskAbort(task, cause)` because that would abort the orchestrator
      log.debug(task.withOrchestratorAndTaskPrefix("Aborted."))
      
      log.debug(withLogPrefix(quorumStatus))
      
      if (tolerance < 0) {
        log.info(withLogPrefix("Tolerance surpassed."))
        // We abort the waiting tasks to ensure that if this orchestrator crashes and is restarted, then it will
        // be restored to the correct state, and no messages are sent to the destinations.
        waitingTasks.values.foreach(_.abort(QuorumImpossibleToAchieve))
        onAbort(Aborted(QuorumImpossibleToAchieve, startId))
      } else if (tolerance == 0 && waitingTasks.isEmpty) {
        // This was the last task and the tolerance has not surpassed.
        // So onFinish is invoked directly, it will terminate the Quorum with a QuorumNotAchieved.
        onFinish()
      }
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
}

/**
  * A task that creates a variable number of tasks and succeeds when `n` tasks finish producing the same result.
  * `n` is calculated with the minimumVotes function.
  * The return type and the message of the tasks must be the same. And their destinations must be different.
  *
  * The last two restrictions are validated in runtime when the quorum is created. If they fail the task will abort.
  * To validate the messages are the same, each task message is compared via `!=` against the other tasks messages.    
  */
class TaskQuorum[R](task: FullTask[R, _])(minimumVotes: MinimumVotes, taskBuilders: Iterable[TaskBuilder[R]])
  extends TaskSpawnOrchestrator[R, Quorum[R]](task)(
    Props(classOf[Quorum[R]], taskBuilders, minimumVotes, task.orchestrator.persistenceId)
  )