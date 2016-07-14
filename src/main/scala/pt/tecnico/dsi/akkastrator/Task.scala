package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorPath}
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

//TODO: find a better name for Task.Status to avoid name collisions with Status and TaskStatus

object Task {
  sealed trait Status
  case object Unstarted extends Status
  case object Waiting extends Status
  case object Finished extends Status
}

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
private[akkastrator] abstract class AbstractTask[TT <: AbstractTask[TT]](orchestrator: AbstractOrchestrator,
                                                                         description: String, dependencies: Set[TT]) {
  type ID <: Id //The type of Id this task handles
  import orchestrator._

  //The index of this task in the orchestrator task list. //TODO: maybe this can be private/protected
  val index: Int

  private final val colors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.RED,
    Console.WHITE
  )
  final def color = colors(index % colors.size)
  def withLoggingPrefix(message: ⇒ String): String = f"$color[$index%02d - $description] $message${Console.RESET}"

  //We always start in the Unstarted status and without an expectedDeliveryId
  status = Unstarted
  expectedDeliveryId = None

  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change
  /** The constructor of the message to be sent. */
  def createMessage(id: ID): Any

  /** If recovery is running just executes `handler`, otherwise persists the `event` and uses `handler` as its handler.*/
  private def recoveryAwarePersist(event: Event)(handler: ⇒ Unit): Unit = {
    if (recoveryRunning) {
      //When we are recovering we do not want to persist the event again.
      //We just want to execute the handler.
      log.info(withLoggingPrefix(s"Recovering ${event.getClass.getSimpleName}."))
      handler
    } else {
      log.debug(withLoggingPrefix(s"Persisting ${event.getClass.getSimpleName}."))
      persist(event) { _ ⇒
        log.debug(withLoggingPrefix(s"Persist successful."))
        handler
      }
    }
  }

  /** If this task is in `status` then `handler` is executed, otherwise a exception will be thrown.*/
  private def ensureInStatus(status: Task.Status, operationName: String)(handler: ⇒ Unit): Unit = status match {
    case `status` ⇒ handler
    case _ ⇒
      val message = s"$operationName can only be invoked when task is $status."
      log.error(withLoggingPrefix(message))
      throw new IllegalStateException(message)
  }

  /** Converts the deliveryId obtained from the deliver method of akka-persistence to the ID this task handles. */
  protected[akkastrator] def deliveryId2ID(deliveryId: DeliveryId): ID
  /**
    * Starts the execution of this task.
    * If this task is already Waiting or Finished an exception will be thrown.
    * We first persist that the message was sent (unless the orchestrator is recovering), then we send it.
    */
  final protected[akkastrator] def start(): Unit = ensureInStatus(Unstarted, "Start") {
    log.info(withLoggingPrefix(s"Starting."))
    recoveryAwarePersist(MessageSent(index)) {
      //First we make sure the orchestrator is ready to deal with the answers from destination
      status = Waiting
      updateCurrentBehavior()

      //Then we send the message to the destination
      deliver(destination) { i ⇒
        if (!recoveryRunning) {
          //When we are recovering this method (the deliver handler) will be run
          //but the message won't be delivered every time so we hide the println to cause less confusion
          log.debug(withLoggingPrefix(s"Delivering message."))
        }
        val deliveryId: DeliveryId = i
        expectedDeliveryId = Some(deliveryId)
        createMessage(deliveryId2ID(deliveryId))
      }
    }
  }

  /**
    * Low-level match. It will behave differently according to the orchestrator in which this task is being created.
    * The parameter `id` is a `Long` and not a `ID` so this method can be invoked in the TaskProxy. */
  def matchId(id: Long): Boolean
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

  //Converts the ID this task handles to the corresponding (CorrelationID, DeliveryID)
  protected[akkastrator] def ID2Ids(id: ID): (CorrelationId, DeliveryId)

  private def innerFinish(receivedMessage: Any, id: ID)(extraActions: ⇒ Unit): Unit = {
    val (correlationId, deliveryId) = ID2Ids(id)
    recoveryAwarePersist(MessageReceived(index, receivedMessage, correlationId, deliveryId)) {
      confirmDelivery(deliveryId.self)
      status = Finished
      extraActions
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
  final def finish(receivedMessage: Any, id: ID): Unit = ensureInStatus(Waiting, "Finish") {
    log.info(withLoggingPrefix(s"Finishing."))
    innerFinish(receivedMessage, id) {
      //This starts tasks that have a dependency on this task
      self ! StartReadyTasks
      //This is invoked to remove this task behavior from the orchestrator
      //This will be performed even if the orchestrator has terminated early
      updateCurrentBehavior()
    }
  }

  /**
    * This will cause this task <b>orchestrator</b> to terminate early.
    *
    * An early termination will have the following effects:
    *  - This task will be finished.
    *  - Every unstarted task will be prevented from starting even if its dependencies have finished.
    *  - Tasks that are waiting will remain untouched and the orchestrator will
    *    still be prepared to handle their responses.
    *  - The method `onFinish` will <b>never</b> be called even if the only tasks needed to finish
    *    the orchestrator are already waiting and their responses are received.
    *  - The method `onEarlyTermination` will be invoked in the orchestrator.
    *
    */
  final def terminateEarly(receivedMessage: Any, id: ID): Unit = ensureInStatus(Waiting, "TerminateEarly") {
    log.info(withLoggingPrefix(s"Terminating Early."))
    innerFinish(receivedMessage, id) {
      //This will prevent unstarted tasks from starting and onFinished from being called.
      earlyTerminated = true
      //This is invoked to remove this task behavior from the orchestrator
      updateCurrentBehavior()
      onEarlyTermination(this.asInstanceOf[T], receivedMessage, tasks.groupBy(_.status))
    }
  }

  /** @return whether this task status is Unstarted and all its dependencies have finished. */
  final def canStart: Boolean = status == Unstarted && dependencies.forall(_.hasFinished)
  /** @return whether this task status is `Waiting`. */
  final def isWaiting: Boolean = status == Waiting
  /** @return whether this task status is `Finished`. */
  final def hasFinished: Boolean = status == Finished

  /** The TaskStatus representation of this task. */
  final def toTaskStatus: TaskStatus = TaskStatus(index, description, status, dependencies.map(_.index))

  private var _status: Task.Status = Unstarted
  /** @return the current status of this Task. */
  final def status: Task.Status = _status
  private def status_=(status: Task.Status): Unit = {
    _status = status
    log.info(withLoggingPrefix(s"Status: $status."))
  }

  private var _expectedDeliveryId: Option[DeliveryId] = None
  /** @return the current expected deliveryId of this Task. */
  final def expectedDeliveryId: Option[DeliveryId] = _expectedDeliveryId
  private def expectedDeliveryId_=(expectedDeliveryId: Option[DeliveryId]): Unit = {
    _expectedDeliveryId = expectedDeliveryId
  }

  override def toString: String =
    f"""Task [$index%02d - $description]:
       |Destination: $destination
       |Status: $status""".stripMargin
}
