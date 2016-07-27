package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorPath, Props}

sealed trait TaskState
case object Unstarted extends TaskState
case class Waiting(expectedDeliveryId: DeliveryId) extends TaskState
case class Finished[R](result: R) extends TaskState
case object TerminatedEarly extends TaskState

/** An immutable view of an Orchestrator Task. */
case class TaskView(description: String, dependencies: Set[Int], destination: ActorPath, state: TaskState)

  /**
    * A task corresponds to sending a message to an actor, handling its response and possibly
    * mutate the internal state of the Orchestrator.
    * The answer(s) to the sent message must be handled in `behavior`.
    * `behavior` must invoke `finish` when no further processing is necessary.
    * The pattern matching inside `behavior` must invoke `matchSenderAndID` to ensure
    * the received message is in fact the one that its waiting to receive.
    * The internal state of the orchestrator might be mutated inside `behavior`.
    *
    * This class is very tightly coupled with Orchestrator and the reverse is also true.
    * Because of this you can only create instances of Task inside an orchestrator.
    */
abstract class Task(val description: String, val dependencies: Set[Task] = Set.empty)(implicit orchestrator: AbstractOrchestrator) {
  //TODO: change the logging to a proper task logging
  import orchestrator.log
  import IdImplicits._
  
  type Result //The type of this task result
  //The index of this task in the orchestrator task list.
  //This also adds this task to the orchestrator
  final val index: Int = orchestrator.addTask(this)

  //These methods aren't final to allow turning of the colors
  val colors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.WHITE
  )
  val color: String = colors(index % colors.size)
  def withLoggingPrefix(message: ⇒ String): String = f"$color[$index%02d - $description] $message${Console.RESET}"

  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change.
  /** The constructor of the message to be sent. */
  def createMessage(id: Long): Any

  /** If recovery is running just executes `handler`, otherwise persists the `event` and uses `handler` as its handler.*/
  private def recoveryAwarePersist(event: Event)(handler: ⇒ Unit): Unit = {
    if (orchestrator.recoveryRunning) {
      // When recovering the event is already persisted no need to persist it again.
      handler
    } else {
      orchestrator.persist(event) { _ ⇒
        log.debug(withLoggingPrefix(s"Persisted ${event.getClass.getSimpleName}."))
        handler
      }
    }
  }

  /**
    * Starts the execution of this task.
    * If this task is already Waiting or Finished an exception will be thrown.
    * We first persist that the message was sent (unless the orchestrator is recovering), then we send it.
    */
  final protected[akkastrator] def start(): Unit = {
    require(canStart, "Start can only be invoked when this task is Unstarted and all of its dependencies have finished.")
    log.info(withLoggingPrefix(s"Starting."))
    recoveryAwarePersist(MessageSent(index)) {
      orchestrator.deliver(destination) { deliveryId ⇒
        //First we make sure the orchestrator is ready to deal with the answers from destination
        state = Waiting(deliveryId)
        orchestrator.updateCurrentBehavior()
        
        if (!orchestrator.recoveryRunning) {
          //When we are recovering this method (the deliver handler) will be run
          //but the message won't be delivered every time so we hide the println to cause less confusion
          log.debug(withLoggingPrefix(s"Delivering message"))
        }
        
        val id = orchestrator.deliveryId2ID(destination, deliveryId)
        createMessage(id.self)
      }
    }
  }
  
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, id)

  /**
    * The behavior of this task. This is akin to the receive method of an actor, except for the fact that an
    * all catching pattern match will cause the orchestrator to fail. For example:
    * {{{
    *   def behavior = Receive {
    *     case m => //Some code
    *   }
    * }}}
    * This will cause the orchestrator to fail because the messages won't be handled by the correct tasks.
    */
  def behavior: Actor.Receive
  
  private def persistAndConfirmDelivery(receivedMessage: Any, id: Long)(continuation: ⇒ Unit): Unit = {
    recoveryAwarePersist(MessageReceived(index, receivedMessage)) {
      val deliveryId = orchestrator.ID2DeliveryId(destination, id).self
      orchestrator.confirmDelivery(deliveryId)
      continuation
    }
  }

  /**
    * Finishes this task, which implies:
    *
    *  1. Tasks that depend on this one will be started.
    *  2. Messages that would be handled by this task will no longer be handled.
    *
    * @param receivedMessage the message which prompted the finish.
    * @param id the id obtained from the message.
    */
  final def finish(receivedMessage: Any, id: Long, result: Result): Unit = {
    require(isWaiting, "Finish can only be invoked when this task is Waiting.")
    log.info(withLoggingPrefix(s"Finishing."))
    persistAndConfirmDelivery(receivedMessage, id) {
      state = Finished(result)
      //Remove this task behavior from the orchestrator to ensure re-sends do not cause the orchestrator
      //to crash due to the require at the top of this method. This means re-sends will cause a "unhandled message"
      //log message.
      orchestrator.updateCurrentBehavior()
      orchestrator.self ! StartReadyTasks
    }
  }

  /**
    * Causes this task and its <b>orchestrator</b> to terminate early. This will have the following effects:
    *  1. This task will change its state to `TerminatedEarly`.
    *  2. Every unstarted task that depends on this one will never be started. This will happen because a task can
    *     only start if its dependencies have finished and this task did not finish.
    *  3. Waiting tasks will be untouched and the orchestrator will still be prepared to handle their responses.
    *  4. The method `onFinish` will <b>never</b> be called. Similarly to the unstarted tasks, onFinish will only
    *     be invoked if all tasks have finished and this task did not finish.
    *  5. The method `onEarlyTermination` will be invoked in the orchestrator.
    *
    */
  final def terminateEarly(receivedMessage: Any, id: Long): Unit = {
    require(isWaiting, "TerminateEarly can only be invoked when this task is Waiting.")
    log.info(withLoggingPrefix(s"Terminating Early."))
    persistAndConfirmDelivery(receivedMessage, id) {
      //This will prevent:
      // · Unstarted tasks, that depend on this one, from starting because canStart on those tasks will never return true
      // · onFinished from being called because the condition `tasks.forall(_.hasFinished)` on the orchestrator will never return true
      // It will also cause this task behavior to be removed from the orchestrator since this task will no longer be waiting.
      state = TerminatedEarly
      //Remove this task behavior from the orchestrator
      orchestrator.updateCurrentBehavior()
      orchestrator.onEarlyTermination(this, receivedMessage, orchestrator.tasks.groupBy(_.state))
    }
  }

  /** @return The result of this task. A task will only have a result if it is finished. */
  final def result: Option[Result] = state match {
    case Finished(r) ⇒ Some(r.asInstanceOf[Result])
    case _ ⇒ None
  }

  /** @return whether this task state is Unstarted and all its dependencies have finished. */
  final def canStart: Boolean = state == Unstarted && dependencies.forall(_.hasFinished)
  final def isWaiting: Boolean = state.isInstanceOf[Waiting]
  final def hasFinished: Boolean = state.isInstanceOf[Finished[_]]

  /** The immutable view of this task. */
  final def toTaskView: TaskView = TaskView(description, dependencies.map(_.index), destination, state)

  private var _state: TaskState = Unstarted
  /** @return the current state of this Task. */
  final def state: TaskState = _state
  private def state_=(state: TaskState): Unit = {
    _state = state
    log.info(withLoggingPrefix(s"State: $state."))
  }

  override def toString: String =
    f"""Task [$index%02d - $description]:
       |Destination: $destination
       |State: $state""".stripMargin

  def canEqual(other: Any): Boolean = other.isInstanceOf[Task]
  
  override def equals(other: Any): Boolean = other match {
    case that: Task ⇒
      (that canEqual this) &&
        index == that.index &&
        destination == that.destination &&
        description == that.description &&
        dependencies == that.dependencies
    case _ ⇒ false
  }
  
  override def hashCode(): Int = {
    val state: Seq[Any] = Seq(index, destination, description, dependencies)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }
}