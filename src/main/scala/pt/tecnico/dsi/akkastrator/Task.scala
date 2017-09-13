package pt.tecnico.dsi.akkastrator

import java.util.concurrent.TimeoutException

import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{Actor, ActorPath, PossiblyHarmful}
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.Event._

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case class Aborted(cause: Throwable) extends State
  case class Finished[R](result: R) extends State
  
  case class Timeout(id: Long) extends PossiblyHarmful
  
  // TODO: maybe this class should be in FullTask companion object
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

// TODO: the requires inside Task.{start, finish, innerAbort} and TaskSpawnOrchestrator might be troublesome since they will crash the orchestrator.

/**
  * A task corresponds to sending a message to an actor, handling its response and possibly
  * mutate the internal _state of the Orchestrator.
  *
  * The answer(s) to the sent message must be handled in `behavior`. `behavior` must invoke `finish` when
  * no further processing is necessary. Or `abort` if the received message will prevent subsequent
  * tasks from executing properly.
  *
  * The pattern matching inside `behavior` must invoke `matchId` to ensure the received message
  * is in fact the one that this task its waiting to receive.
  *
  * The internal _state of the orchestrator might be mutated inside `behavior`.
  *
  * This class is very tightly coupled with Orchestrator and the reverse is also true. See [[AbstractOrchestrator]] for
  * more details on why that is the case.
  */
abstract class Task[R](val task: FullTask[_, _]) { // Unfortunately we cannot make the DSL work with `val task: FullTask[R, _]`
  //This field exists to allow mutating the internal state of the orchestrator easily from inside behavior.
  val orchestrator: AbstractOrchestrator[_] = task.orchestrator
  
  import orchestrator.log
  import orchestrator.ID
  import IdImplicits._
  
  private[this] var _expectedID: ID = _ // Can you see the null? Blink and you'll miss it.
  def expectedID: Option[ID] = Option(_expectedID) // Ensure the null does not escape
  
  private[this] var _state: Task.State = Unstarted
  def state: Task.State = _state
  
  /** The ActorPath to whom this task will send the message(s). This must be a val because the destination cannot change. */
  val destination: ActorPath
  /** The constructor of the message to be sent. It must always return the same message, only the id must be different.
    * If this Task is to be used inside a TaskQuorum then the created message should also implement `equals`. */
  def createMessage(id: Long): Serializable
  
  final def start(): Unit = {
    require(state == Unstarted, "Start can only be invoked when this task is Unstarted.")
    log.info(withOrchestratorAndTaskPrefix(s"Starting."))
    
    val taskStarted = TaskStarted(task.index)
    orchestrator.recoveryAwarePersist(taskStarted, withTaskPrefix(s"Persisted $taskStarted")) {
      orchestrator.deliver(destination) { deliveryId =>
        _expectedID = orchestrator.computeID(destination, deliveryId)
        _state = Waiting
        // The next line makes sure the orchestrator is ready to deal with the answers from destination.
        orchestrator.taskStarted(task, this)
  
        log.debug(withOrchestratorAndTaskPrefix(if (orchestrator.recoveryRunning) {
          // "Possibly" because when recovering the deliver handler will be run but the message won't be delivered every time
          "Possibly delivering message. Possibly because we are recovering."
        } else {
          "Delivering message."
        }))
  
        scheduleTimeout(_expectedID)
        
        createMessage(_expectedID.self)
      }
    }
  }
  final def scheduleTimeout(id: ID): Unit = task.timeout match {
    case f: FiniteDuration if f >= Duration.Zero =>
      import orchestrator.context.system
      system.scheduler.scheduleOnce(f) {
        // We would like to do `orchestrator.self.tell(Timeout(id.self), destination)` however
        // we don't have the destination ActorRef. And since we want to make handling Timeout inside behavior
        // uniform with the rest of the messages (aka we want to make the user invoke matchId for Timeout)
        // we are forced to make an exception inside matchId to detect when we are handling a Timeout.
        // The exception is matching the sender against `orchestrator.self.path` which signals we are handling a timeout.
        orchestrator.self.tell(Timeout(id.self), orchestrator.self)
      }(system.dispatcher)
    case _ => // We do nothing because the timeout is either negative, 0, or infinite.
  }
  
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, orchestrator.toID(id))
  
  /**
    * The behavior of this task. This is akin to the receive method of an actor with the following exceptions:
    *  · An all catching pattern match is prohibited since it will cause the orchestrator to fail.
    *  · Every case must check if `matchId` returns true.
    *    This ensures the received message was in fact destined to this task.
    *    This choice of implementation allows the messages to have a free form, as it is the user that
    *    is responsible for extracting the `id` from the message.
    *  · Either `finish`, `abort` or `timeout` must be invoked after handling each response.
    *    However `timeout` cannot be invoked when handling the `Timeout` message.
    *  · The internal state of the orchestrator might be changed while handling each response using
    *    `orchestrator.state = //Your new _state`
    *
    * Example of a well formed behavior: {{{
    *   case m @ Success(result, id) if matchId(id) =>
    *     orchestrator.state = //A new _state
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
    // This is the default timeout handling logic.
    case Timeout(id) if matchId(id) => abort(new TimeoutException())
  }
  
  private final def persistAndConfirmDelivery(event: Event)(continuation: => Unit): Unit = {
    // This method is only invoked from inside finish and abort. These methods are only invoked if
    // matchId returned true. So we already validated the id and can safely use _expectedID.
    orchestrator.recoveryAwarePersist(event, withTaskPrefix(s"Persisted $event")) {
      orchestrator.confirmDelivery(orchestrator.deliveryIdOf(destination, _expectedID).self)
      continuation
    }
  }

  // TODO: we could make finish/abort idempotent, aka lift the contract stating:
  //       "Finishing/Aborting an already finished/aborted task will throw an exception."
  
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
    * @param result the result this task will produce. This is the value that the tasks that depend on this one will see.
    */
  final def finish(result: R): Unit = {
    require(state == Waiting, "Finish can only be invoked when this task is Waiting.")
    log.info(withOrchestratorAndTaskPrefix(s"Finishing."))
    persistAndConfirmDelivery(TaskFinished(task.index, result)) {
      _state = Finished(result)
      // The next line makes sure the orchestrator no longer deals with the answers from destination.
      // This means that if destination re-sends an answer the orchestrator will treat it as an unhandled message.
      orchestrator.taskFinished(task)
  
      // TODO: maybe we could keep a list of finished/aborted tasks in order to catch the re-sends and ignore them like:
      //   final def behaviorIgnoringResends: Actor.Receive = { case m if behavior.isDefinedAt(m) => /* Ignore the message */ }
      // Is it really worth it to store an additional list in the orchestrator just to filter some messages from the log?
  
      // Notify the tasks that depend on this one that this task has finished.
      task.notifyDependents()
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
    *  Aborting an already aborted task will throw an exception.
    *  
    * @param cause what caused the abort to be invoked.
    */
  final def abort(cause: Throwable): Unit = innerAbort(cause) {
    orchestrator.onTaskAbort(task, cause)
  }
  
  private[akkastrator] final def innerAbort(cause: Throwable)(afterAbortContinuation: => Unit): Unit = {
    require(state == Waiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withOrchestratorAndTaskPrefix(s"Aborting due to exception: $cause."))
    persistAndConfirmDelivery(TaskAborted(task.index, cause)) {
      _state = Aborted(cause)
      
      orchestrator.taskAborted(task)
      
      // We do not invoke task.notifyDependents which guarantees the contract that
      // "Every unstarted task that depends on this one will never be started."
      
      afterAbortContinuation
    }
  }
  
  // These are shortcuts
  def withTaskPrefix(message: => String): String = task.withTaskPrefix(message)
  def withOrchestratorAndTaskPrefix(message: => String): String = task.withOrchestratorAndTaskPrefix(message)
  
  override def toString: String = s"Task($expectedID, $state, $destination)"
}