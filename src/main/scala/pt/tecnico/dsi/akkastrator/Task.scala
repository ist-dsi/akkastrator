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
}

// TODO: maybe we could leverage the Task.State and implement task in a more functional way, aka, remove its internal state.
// Task could be a inner class of FullTask. This would simplify a lot the usability of FullTask and Task. Prototype how it would influence refactoring of Tasks 
// The requires inside Task.{start, finish, innerAbort} and TaskSpawnOrchestrator might be troublesome since they will crash the orchestrator.

/**
  * A task corresponds to sending a message to an actor, handling its response and possibly
  * mutate the internal state of its Orchestrator.
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
  * @param task
  * @tparam R the return type of this Task. 
  */
abstract class Task[R](val task: FullTask[R, _]) {
  import task.orchestrator
  import task.orchestrator.ID
  import IdImplicits._

  // Facility to allow logging from within Task
  val log = orchestrator.log
  
  private[this] var _expectedID: ID = _ // Can you see the null? Blink and you'll miss it.
  final def expectedID: Option[ID] = Option(_expectedID) // Ensure the null does not escape
  
  private[this] var _state: Task.State = Unstarted
  final def state: Task.State = _state
  
  /** The [[akka.actor.ActorPath]] to whom this task will send the message(s).
    * This must be a value because the destination cannot change. */
  val destination: ActorPath
  // It would be awesome if `id` had type ID. Unfortunately that breaks the reusability of Task.
  /** The constructor of the message to be sent. It must always return the same message, only the id must be different.
    * If this Task is to be used inside a TaskQuorum then the created message should also implement `equals`. */
  def createMessage(id: Long): Serializable
  
  final def start(): Unit = {
    require(state == Unstarted, "Start can only be invoked when this task is Unstarted.")
    log.info(withOrchestratorAndTaskPrefix(s"Starting."))
    
    val taskStarted = TaskStarted(task.index)
    recoveryAwarePersist(taskStarted) {
      orchestrator.deliver(destination) { deliveryId =>
        _expectedID = orchestrator.computeID(destination, deliveryId)
        _state = Waiting
  
        log.debug(withOrchestratorAndTaskPrefix(if (orchestrator.recoveryRunning) {
          // "Possibly" because when recovering the deliver handler will be run but the message won't be delivered every time
          "Possibly (because we are recovering) delivering message."
        } else {
          "Delivering message."
        }))
  
        // The next line makes sure the orchestrator is ready to deal with the answers from destination (and the timeout).
        orchestrator.taskStarted(task, this)
  
        scheduleTimeout(_expectedID)
        
        createMessage(_expectedID.self)
      }
    }
  }
  final def scheduleTimeout(id: ID): Unit = task.timeout match {
    case f: FiniteDuration if f > Duration.Zero =>
      import orchestrator.context.system
      system.scheduler.scheduleOnce(f) {
        // We would like to do `orchestrator.self.tell(Timeout(id.self), destination)` however
        // we don't have the destination ActorRef. And since we want to make handling Timeout inside behavior
        // uniform with the rest of the messages (aka we want to make the user invoke matchId for Timeout)
        // we are forced to make an exception inside matchId to detect when we are handling a Timeout.
        // The exception is matching the sender against `orchestrator.self.path` which signals we are handling a timeout.
        orchestrator.self.tell(Timeout(id.self), orchestrator.self)
      }(system.dispatcher)
    case f: FiniteDuration =>
      log.warning(withOrchestratorAndTaskPrefix(s"Invalid timeout = $f. Expected a finite duration > Duration.Zero. Interpreting as Duration.Inf."))
    case _ => // We do nothing because the timeout is infinite.
  }
  
  // It would be awesome if `id` had type ID. Unfortunately that breaks the reusability of Task.
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, id)
  
  /**
    * The behavior of this task. This is akin to the receive method of an actor with the following exceptions:
    *  · An all catching pattern match is prohibited since it will cause the orchestrator to fail.
    *  · Every case must check if `matchId` returns true.
    *    This ensures the received message was in fact destined to this task.
    *    This choice of implementation allows the messages to have a free form, as it is the user that
    *    is responsible for extracting the `id` from the message.
    *  · Either `finish` or `abort` must be invoked after handling each response.
    *
    * Example of a well formed behavior: {{{
    *   case Success(result, id) if matchId(id) =>
    *     orchestrator.state = // a new state
    *     finish("This task result") // The result is the value that the tasks that depend on this one will see.
    *   case SomethingWentWrong(why, id) if matchId(id) =>
    *     abort(why)
    *   case Timeout(id) =>
    *     abort(anError)
    * }}}
    *
    */
  def behavior: Actor.Receive
  
  final def behaviorHandlingTimeout: Actor.Receive = behavior orElse {
    // This is the default timeout handling logic.
    case Timeout(id) if matchId(id) => abort(new TimeoutException())
  }
  
  final def recoveryAwarePersist(event: Any)(handler: => Unit): Unit = {
    if (orchestrator.recoveryRunning) {
      // When recovering the event is already persisted no need to persist it again.
      log.info(withOrchestratorAndTaskPrefix(s"Recovering $event."))
      handler
    } else {
      orchestrator.persist(event) { _ =>
        log.debug(withOrchestratorAndTaskPrefix(s"Persisted $event."))
        handler
      }
    }
  }
  private final def persistAndConfirmDelivery(event: Event)(handler: => Unit): Unit = {
    // This method is only invoked from inside finish and abort. These methods are only invoked if
    // matchId returned true. So we already validated the id and can safely use _expectedID.
    recoveryAwarePersist(event) {
      orchestrator.confirmDelivery(orchestrator.deliveryIdOf(destination, _expectedID).self)
      handler
    }
  }
  
  // TODO: maybe we could keep a list of finished/aborted tasks inside orchestrator in order to catch the re-sends and ignore them
  // Is it really worth it to store an additional list in the orchestrator just to filter some messages from the log?
    
  /**
    * Finishes this task, which implies:
    *
    *  1. This task will change its state to `Finished`.
    *  2. Tasks that depend on this one will be started.
    *  3. Re-sends from `destination` will no longer be handled by the orchestrator.
    *     If destinations re-sends its answer it will be logged as an unhandled message.
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
  final def abort(cause: Throwable): Unit = {
    require(state == Waiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withOrchestratorAndTaskPrefix(s"Aborting due to exception: $cause."))
    persistAndConfirmDelivery(TaskAborted(task.index, cause)) {
      _state = Aborted(cause)
      // The next line makes sure the orchestrator no longer deals with the answers from destination.
      // This means that if destination re-sends an answer the orchestrator will treat it as an unhandled message.
      orchestrator.taskAborted(task, cause)
    
      // We do not invoke task.notifyDependents which guarantees the contract that
      // "Every unstarted task that depends on this one will never be started."
    }
  }
  
  // This is a shortcut
  final def withOrchestratorAndTaskPrefix(message: => String): String = task.withOrchestratorAndTaskPrefix(message)
  
  override def toString: String = s"Task(${task.index}, $expectedID, $state, $destination)"
}