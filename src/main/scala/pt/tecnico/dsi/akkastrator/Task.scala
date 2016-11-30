package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorPath, PossiblyHarmful}
import pt.tecnico.dsi.akkastrator.Orchestrator.{SaveSnapshot, TaskReport}
import pt.tecnico.dsi.akkastrator.Task._

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case class Aborted(cause: Exception) extends State
  // It would be nice to find a way to ensure the type parameter R of this class matches with the type parameter R of Task
  case class Finished[R](result: R) extends State
  
  case class Timeout(id: Long) extends PossiblyHarmful
}

// Maybe we could leverage the Task.State and implement task in a more functional way, aka, remove its internal state.


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
  * This class is very tightly coupled with Orchestrator and the reverse is also true.
  */
abstract class Task[R](val task: FullTask[_, _]) {
  import IdImplicits._
  import task.index
  import task.orchestrator.log
  
  private[akkastrator] var expectedDeliveryId: Option[DeliveryId] = None
  private[akkastrator] var state: Task.State = Unstarted
  
  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change.
  /** The constructor of the message to be sent. It must always return the same message, only the id must be different.
    * If this Task is to be used inside a TaskQuorum then the created message should also implement `equals`. */
  def createMessage(id: Long): Any
  
  final protected[akkastrator] def start(): Unit = {
    require(state == Unstarted, "Start can only be invoked when this task is Unstarted.")
    log.info(withLogPrefix(s"Starting."))
    orchestrator.recoveryAwarePersist(MessageSent(task.index)) {
      if (!orchestrator.recoveryRunning) {
        log.debug(withLogPrefix(s"Persisted MessageSent."))
      }
      
      orchestrator.deliver(destination) { deliveryId =>
        // First we make sure the orchestrator is ready to deal with the answers from destination.
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
  
        val id = orchestrator.deliveryId2ID(destination, deliveryId).self
        
        // Schedule the timeout
        if (task.timeout.isFinite()) {
          import orchestrator.context.system
          system.scheduler.scheduleOnce(FiniteDuration(task.timeout.length, task.timeout.unit)) {
            orchestrator.self ! orchestrator.TaskTimedOut(index, id)
          }(system.dispatcher)
        }
        
        createMessage(id)
      }
    }
  }
  
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, id)
  
  //This field exists to allow mutating the internal state of the orchestrator easily from inside behavior.
  val orchestrator: AbstractOrchestrator[_] = task.orchestrator
  
  /**
    * The behavior of this task. This is akin to the receive method of an actor with the following exceptions:
    *  · An all catching pattern match is prohibited since it will cause the orchestrator to fail.
    *  · Every case must check if `matchId` returns true, except for the `Timeout` message.
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
  
  private final def persistAndConfirmDelivery(receivedMessage: Serializable, id: Long)(continuation: => Unit): Unit = {
    orchestrator.recoveryAwarePersist(MessageReceived(task.index, receivedMessage)) {
      if (!orchestrator.recoveryRunning) {
        log.debug(withLogPrefix(s"Persisted MessageReceived."))
      }
      val deliveryId = orchestrator.ID2DeliveryId(destination, id).self
      orchestrator.confirmDelivery(deliveryId)
      continuation
    }
  }

  /**
    * Finishes this task, which implies:
    *
    *  1. Tasks that depend on this one will be started.
    *  2. Re-sends from `destination` will no longer be handled by `behavior`.
    *     The re-send message will be handled as an unhandled message, aka the `unhandled`
    *     method will be invoked in the orchestrator.
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
  
      // Notify the tasks that depend on this one, that this one has finished.
      task.notifyDependents()
  
      //TODO: does invoking onFinish inside the persistHandler cause any problem?
      if (orchestrator.finishedTasks == orchestrator.tasks.size) {
        orchestrator.onFinish()
      }
    }
  }

  /**
    * Causes this task to abort. This will have the following effects:
    *  1. This task will change its state to `Aborted`.
    *  2. Every unstarted task that depends on this one will never be started. This will happen because a task can
    *     only start if its dependencies have finished and this task did not finish, it aborted.
    *  3. Waiting tasks or tasks which do not have this task as a dependency will continue to be executed or be started,
    *     unless the orchestrator is stopped, which can be performed in the `onAbort` callback of the orchestrator.
    *  4. The method `onFinish` will <b>never</b> be called. Similarly to the unstarted tasks, onFinish will only
    *     be invoked if all tasks have finished.
    *  5. The method `onAbort` will be invoked in the orchestrator.
    *
    * @param receivedMessage the message which prompted the abort.
    * @param id the id obtained from the message.
    * @param cause what caused the abort to be invoked.
    */
  final def abort(receivedMessage: Serializable, id: Long, cause: Exception): Unit = {
    innerAbort(receivedMessage, id, cause) {
      orchestrator.onAbort(task, receivedMessage, cause, orchestrator.tasks.groupBy(_.state))
  
      // Unlike finish we do NOT invoke:
      // · orchestrator.onTaskFinish(this) - thus ensuring onTaskFinish is not invoked for aborted tasks.
      // · task.notifyDependents() - thus keeping good on the promise that unstarted tasks that depend on this one will never be started.
      // . orchestrator.onFinish - thus ensuring onFinish is never called when a task aborts.
    }
  }
  
  private[akkastrator] final def innerAbort(receivedMessage: Serializable, id: Long, cause: Exception)(afterAbortContinuation: => Unit): Unit = {
    require(state == Waiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withLogPrefix(s"Aborting due to $cause."))
    persistAndConfirmDelivery(receivedMessage, id) {
      expectedDeliveryId = None
      state = Aborted(cause)
      orchestrator.waitingTasks -= index
      orchestrator.context become orchestrator.computeCurrentBehavior()
      
      afterAbortContinuation
    }
  }
  
  final def timeout(receivedMessage: Serializable, id: Long): Unit = {
    require(Timeout(id) != receivedMessage, "Cannot invoke timeout when handling the timeout.")
    require(state == Waiting, "innerTimeout can only be invoked when this task is Waiting.")
    behavior.applyOrElse(Timeout(id), { timeout: Timeout =>
      //behavior does not handle timeout. So we abort it.
      abort(receivedMessage = timeout, id = id, cause = TimedOut)
    })
  }
  
  // These are shorcuts
  def withLogPrefix(message: => String): String = task.withLogPrefix(message)
  def toTaskReport: TaskReport[R] = task.toTaskReport.asInstanceOf[TaskReport[R]]
  
  override def toString: String = s"Task($expectedDeliveryId, $state, $destination)"
}