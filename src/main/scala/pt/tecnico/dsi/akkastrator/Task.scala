package pt.tecnico.dsi.akkastrator

import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorPath, PossiblyHarmful}
import pt.tecnico.dsi.akkastrator.Orchestrator.SaveSnapshot
import pt.tecnico.dsi.akkastrator.Task._

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case class Aborted(cause: Exception) extends State
  case class Finished[R](result: R) extends State
  
  case class Timeout(id: Long) extends PossiblyHarmful
  
  /**
    * An immutable representation (a report) of a Task in a given moment of time.
    *
    * @param description a text that describes the task in a human readable way. Or a message key to be used in internationalization.
    * @param dependencies the indexes of the tasks that must have finished in order for the task to be able to start.
    * @param state the current state of the task.
    * @param destination the destination of the task. If the task hasn't started this will be a None.
    * @param result the result of the task. If the task hasn't finished this will be a None.
    * @tparam R the type of the result.
    */
  case class Report[R](description: String, dependencies: Seq[Int], state: Task.State, destination: Option[ActorPath], result: Option[R])
}

// Maybe we could leverage the Task.State and implement task in a more functional way, aka, remove its internal state.

// TODO: the requires inside Task.{start, finish, innerAbort} and TaskSpawnOrchestrator might be troublesome since
// they will crash the orchestrator.

/**
  * A task corresponds to sending a message to an actor, handling its response and possibly
  * mutate the internal state of the Orchestrator.
  *
  * The answer(s) to the sent message must be handled in `behavior`. `behavior` must invoke `finish` when
  * no further processing is necessary. Or `abort` if the received message will prevent subsequent
  * tasks from executing properly.
  *
  * The pattern matching inside `behavior` must invoke `matchId` to ensure the received message
  * is in fact the one that this task its waiting to receive.
  *
  * The internal state of the orchestrator might be mutated inside `behavior`.
  *
  * This class is very tightly coupled with Orchestrator and the reverse is also true. See [[AbstractOrchestrator]] for
  * more details on why that is the case.
  */
abstract class Task[R](val task: FullTask[_, _]) {
  import IdImplicits._
  import task.index
  import task.orchestrator.log
  
  private[akkastrator] var expectedId: Option[Id] = None
  private[akkastrator] var expectedDeliveryId: Option[DeliveryId] = None
  private[akkastrator] var state: Task.State = Unstarted
  
  /**
    * The ActorPath to whom this task will send the message(s).
    * This must be a val because the destination cannot change.
    * */
  val destination: ActorPath
  /** The constructor of the message to be sent. It must always return the same message, only the id must be different.
    * If this Task is to be used inside a TaskQuorum then the created message should also implement `equals`. */
  def createMessage(id: Long): Serializable
  
  final protected[akkastrator] def start(): Unit = {
    require(state == Unstarted, "Start can only be invoked when this task is Unstarted.")
    log.info(withLogPrefix(s"Starting."))
    orchestrator.recoveryAwarePersist(MessageSent(task.index)) {
      if (!orchestrator.recoveryRunning) {
        log.debug(withLogPrefix(s"Persisted MessageSent."))
      }
      
      orchestrator.deliver(destination) { deliveryId =>
        // First we make sure the orchestrator is ready to deal with the answers from destination.
        val id = orchestrator.deliveryId2ID(destination, deliveryId)
        expectedId = Some(id)
        expectedDeliveryId = Some(deliveryId)
        state = Waiting
        orchestrator.unstartedTasks -= index
        orchestrator.waitingTasks += index -> this
        orchestrator.context become orchestrator.computeCurrentBehavior()
  
        // When we are recovering this method (the deliver handler) will be run
        // but the message won't be delivered every time so we hide the println to cause less confusion
        if (!orchestrator.recoveryRunning) {
          log.debug(withLogPrefix("(Possibly) delivering message."))
        }
  
        // Schedule the timeout
        if (task.timeout.isFinite) {
          import orchestrator.context.system
          system.scheduler.scheduleOnce(FiniteDuration(task.timeout.length, task.timeout.unit)) {
            // We would like to do `orchestrator.self.tell(Timeout(id.self), destination)` however
            // we don't have the destination ActorRef. And since we want make handling a Timeout uniform with the
            // rest of the messages, that is, inside behavior invoking matchId, we are forced to make an exception
            // inside matchId to detect when we are handling a Timeout and not match the destination.
            // Instead we match against the `orchestrator.timeouterActorRef` which signals we are handling a timeout.
            orchestrator.self.tell(Timeout(id.self), orchestrator.timeouterActorRef)
          }(system.dispatcher)
        }
        
        createMessage(id.self)
      }
    }
  }
  
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, id)
  
  //This field exists to allow mutating the internal state of the orchestrator easily from inside behavior.
  val orchestrator: AbstractOrchestrator[_] = task.orchestrator
  
  /**
    * The behavior of this task. This is akin to the receive method of an actor with the following exceptions:
    *  · An all catching pattern match is prohibited since it will cause the orchestrator to fail.
    *  · Every case must check if `matchId` returns true.
    *    This ensures the received message was in fact destined to this task.
    *    This choice of implementation allows the messages to have a free form, as it is the user that
    *    is responsible for extracting the `id` from the message.
    *  · Either `finish`, `abort` or `timeout` must be invoked after handling each response.
    *    Although `timeout` cannot be invoked when handling the `Timeout` message.
    *  · The internal state of the orchestrator might be changed while handling each response using
    *    `orchestrator.state = //Your new state`
    *
    * Example of a well formed behavior: {{{
    *   case m @ Success(result, id) if matchId(id) =>
    *     orchestrator.state = //A new state
    *     finish(m, id, result = "This task result") // The result is the value that the tasks that depend on this one will see.
    *   case m @ SomethingWentWrong(why, id) if matchId(id) =>
    *     abort(m, id, why)
    *   case m @ Timeout(id) =>
    *     abort(m, id, anError)
    * }}}
    *
    */
  def behavior: Actor.Receive
  
  final def behaviorHandlingTimeout: Actor.Receive = behavior orElse {
    // If the user specified behavior does not handle Timeout this is the default timeout handling logic.
    case m @ Timeout(id) => abort(receivedMessage = m, id, cause = new TimeoutException())
  }
  
  private final def persistAndConfirmDelivery(receivedMessage: Serializable, id: Long)(continuation: => Unit): Unit = {
    orchestrator.recoveryAwarePersist(MessageReceived(task.index, receivedMessage)) {
      if (!orchestrator.recoveryRunning) {
        log.debug(withLogPrefix("Persisted MessageReceived."))
      }
      val deliveryId = orchestrator.ID2DeliveryId(destination, id).self
      orchestrator.confirmDelivery(deliveryId)
      continuation
    }
  }

  /**
    * Finishes this task, which implies:
    *
    *  1. This task will change its state to `Finished`.
    *  2. Tasks that depend on this one will be started.
    *  3. Re-sends from `destination` will no longer be handled by `behavior`. If destinations re-sends its answer
    *     it will be logged as an unhandled message.
    *  4. The method `onTaskFinish` will be invoked on the orchestrator.
    *
    *  Finishing an already finished task will throw an exception.
    *
    * @param receivedMessage the message which prompted the finish.
    * @param id the id obtained from the message.
    * @param result the result this task will produce. This is the value that the tasks that depend on this one will see.
    */
  final def finish(receivedMessage: Serializable, id: Long, result: R): Unit = {
    require(state == Waiting, "Finish can only be invoked when this task is Waiting.")
    log.info(withLogPrefix(s"Finishing."))
    persistAndConfirmDelivery(receivedMessage, id) {
      // First we make sure the orchestrator no longer deals with the answers from destination.
      // By removing this task behavior from the orchestrator we ensure re-sends do not cause the orchestrator to crash due to the
      // require at the top of this method. This means re-sends will cause a "unhandled message" log message.
      expectedId = None
      expectedDeliveryId = None
      state = Finished(result)
      orchestrator.waitingTasks -= index
      orchestrator.finishedTasks += 1
      orchestrator.context become orchestrator.computeCurrentBehavior()
  
      // This method is invoked whenever a task finishes, so it is a very appropriate location to place
      // the computation of whether we should perform an automatic snapshot.
      // It is lastSequenceNr % (saveSnapshotEveryXMessages * 2) because we persist MessageSent and MessageReceived,
      // however we are only interested in MessageReceived. This will roughly correspond to every X MessageReceived.
      if (orchestrator.saveSnapshotRoughlyEveryXMessages > 0 &&
        orchestrator.lastSequenceNr % (orchestrator.saveSnapshotRoughlyEveryXMessages * 2) == 0) {
        orchestrator.self ! SaveSnapshot
      }
  
      orchestrator.onTaskFinish(task)
  
      // Notify the tasks that depend on this one, that this task has finished.
      task.notifyDependents()
  
      //TODO: does invoking onFinish inside the persistHandler cause any problem?
      if (orchestrator.finishedTasks == orchestrator.tasks.size) {
        orchestrator.onFinish()
      }
    }
  }

  /**
    * Aborts this task, which implies:
    *
    *  1. This task will change its state to `Aborted`.
    *  2. Every unstarted task that depends on this one will never be started. This will happen because a task can
    *     only start if its dependencies have finished.
    *  3. Waiting tasks or tasks which do not have this task as a dependency will remain untouched,
    *     unless the orchestrator is stopped or `context.become` is invoked in the `onTaskAbort`/`onAbort`
    *     callbacks of the orchestrator.
    *  4. The method `onTaskAbort` will be invoked in the orchestrator.
    *  5. The method `onFinish` in the orchestrator will never be invoked since this task did not finish.
    *
    * @param receivedMessage the message which prompted the abort.
    * @param id the id obtained from the message.
    * @param cause what caused the abort to be invoked.
    */
  final def abort(receivedMessage: Serializable, id: Long, cause: Exception): Unit = {
    innerAbort(receivedMessage, id, cause) {
      orchestrator.onTaskAbort(task, receivedMessage, cause)
    }
  }
  
  private[akkastrator] final def innerAbort(receivedMessage: Serializable, id: Long, cause: Exception)(afterAbortContinuation: => Unit): Unit = {
    require(state == Waiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withLogPrefix(s"Aborting due to exception: $cause."))
    persistAndConfirmDelivery(receivedMessage, id) {
      expectedId = None
      expectedDeliveryId = None
      state = Aborted(cause)
      orchestrator.waitingTasks -= index
      orchestrator.context become orchestrator.computeCurrentBehavior()
      
      afterAbortContinuation
    }
  }
  
  // This is a shorcut
  def withLogPrefix(message: => String): String = task.withLogPrefix(message)
  
  override def toString: String = s"Task($expectedDeliveryId, $state, $destination)"
}