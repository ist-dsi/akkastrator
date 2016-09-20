package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.TaskQuorum.InnerOrchestrator

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
  class InnerOrchestrator[R](tasksCreator: ⇒ Seq[AbstractOrchestrator[_] ⇒ Task[R]], minimumVotes: MinimumVotes,
                             outerOrchestratorPersistenceId: String) extends Orchestrator[Seq[R]] {
    def persistenceId: String = s"$outerOrchestratorPersistenceId-${self.path.name}"
    
    require(tasksCreator.nonEmpty, "TasksCreator must create at least one task.")
    
    val distinctDestinationsLength = tasks.map(_.destination).distinct.length
    require(tasks.length == distinctDestinationsLength, "TasksCreator must generate tasks where every destination is distinct.")
    require(tasks.forall(_.dependencies.isEmpty), "TasksCreator must generate tasks without dependencies.")
    require(tasks.map(_.createMessage(1L)).distinct.length == 1, "TasksCreator must generate tasks with the same message.")
    
    tasksCreator.map(creator ⇒ creator(this))
    
    val votesToAchieveQuorum = minimumVotes(tasks.length)
    private var receivedVotes: Int = tasks.count(_.hasFinished)
    
    override def onTaskFinish(finishedTask: Task[_]): Unit = {
      receivedVotes += 1
      if (receivedVotes >= votesToAchieveQuorum) {
        //We already achieved a quorum. So now we want to abort all the tasks that are still waiting.
        //But not cause the orchestrator to abort.
        tasks.filter(_.isWaiting).foreach { task ⇒
          log.info(task.withLoggingPrefix(s"Aborting because quorum has already been achieved."))
          //We know get will succeed because the task is waiting
          //receivedMessage is set to None to make it more obvious that we did not receive a message for this task
          task.persistAndConfirmDelivery(receivedMessage = None, task.expectedDeliveryId.get.self) {
            task.expectedDeliveryId = None
            task.state = Task.Aborted
            updateCurrentBehavior()
          }
        }
        //Since we got the quorum we can finish the orchestrator
        onFinish()
      }
    }
    
    override def onFinish(): Unit = {
      log.info(s"${self.path.name} Finished!")
      //We just return the obtained results
      val results: IndexedSeq[R] = tasks.flatMap(_.result).map(_.asInstanceOf[R])
      context.parent ! TasksFinished(results, startId)
      context stop self
    }
  
    //If any task on the quorum causes an abort, the inner orchestrator will be aborted,
    //which in turn will cause the TaskSpawnOrchestrator to abort
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
class TaskQuorum[R](tasksCreator: ⇒ Seq[AbstractOrchestrator[_] ⇒ Task[R]], minimumVotes: MinimumVotes,
                    description: String, dependencies: Set[Task[_]] = Set.empty)
                   (implicit orchestrator: AbstractOrchestrator[_])
  extends TaskSpawnOrchestrator[Seq[R], InnerOrchestrator[R]](
    Props(classOf[InnerOrchestrator[R]], tasksCreator, minimumVotes, orchestrator.persistenceId),
    description,
    dependencies)(classTag[InnerOrchestrator[R]], orchestrator)