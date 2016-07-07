package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorPath}
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

import scala.collection.immutable.SortedMap

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
abstract class Task(val description: String, val dependencies: Set[Task] = Set.empty[Task])
                   (private implicit val orchestrator: Orchestrator) {
  import orchestrator._
  final val index = addTask(this)

  private final val colors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.RED,
    Console.WHITE
  )
  private final val color = colors(index % colors.size)
  def withLoggingPrefix(message: ⇒ String): String = f"$color[$index%02d - $description] $message${Console.RESET}"

  //The task always starts in the Unstarted status and without an expectedDeliveryId
  status = Unstarted
  expectedDeliveryId = None

  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change
  /** The constructor of the message to be sent. */
  def createMessage(correlationId: CorrelationId): Any

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

  private def ensureInStatus(status: Task.Status, operationName: String)(handler: ⇒ Unit): Unit = status match {
    case `status` ⇒ handler
    case _ ⇒
      val message = s"$operationName can only be invoked when task is $status."
      log.error(withLoggingPrefix(message))
      throw new IllegalStateException(message)
  }

  private def getDeliveryId(correlationId: CorrelationId): DeliveryId = {
    state.idsPerDestination.get(destination).flatMap(_.get(correlationId)) match {
      case Some(deliveryId) ⇒ deliveryId
      case None ⇒
        throw new IllegalArgumentException(
          s"""Could not obtain the delivery id for:
             |\tDestination: $destination
             |\tCorrelationId: $correlationId""".stripMargin)
    }
  }

  /**
   * Starts the execution of this task.
   * If this task is already Waiting or Finished an error will be logged.
   *
   * We first persist that the message was sent, then we send it.
   * If the Orchestrator is recovering then we just send the message because there is
   * no need to persist that the message was sent.
   */
  final protected[akkastrator] def start(): Unit = ensureInStatus(Unstarted, "Start") {
    log.info(withLoggingPrefix(s"Starting."))

    recoveryAwarePersist(MessageSent(index)) {
      //First we make sure the orchestrator is ready to deal with the answers from destination
      status = Waiting
      updateCurrentBehavior()

      //Then we send the message to the destination
      deliver(destination) { deliveryId ⇒
        if (!recoveryRunning) {
          log.debug(withLoggingPrefix(s"Delivering message."))
        }
        expectedDeliveryId = Some(deliveryId)

        //Compute the correlationId and store it in the state
        val correlationId = state.getIdsFor(destination).keySet.lastOption.map(_ + 1).getOrElse(0L)
        state = state.updatedIdsPerDestination(destination, correlationId -> deliveryId)

        createMessage(correlationId)
      }
    }
  }

  /**
   * @param correlationId the correlationId obtained from the received message.
   * @return true if
    *         · This task status is Waiting
    *         · The actor path of the sender is the same as `destination`.
    *         · The delivery id resolved from the correlation id is the expected delivery id for this task.
    *        false otherwise.
    * */
  final def matchSenderAndId(correlationId: CorrelationId): Boolean = {
    lazy val senderPath = sender().path
    lazy val deliveryId = getDeliveryId(correlationId)

    val matches = status == Waiting &&
      (if (recoveryRunning) true else senderPath == destination) &&
      expectedDeliveryId.contains(deliveryId)

    log.debug(withLoggingPrefix {
      val length = senderPath.toString.length max destination.toString.length
      String.format(
        s"""MatchSenderAndId:
           |CorrelationId: $correlationId resolved to DeliveryId: $deliveryId
           |       FIELD │ %${length}s │ EXPECTED VALUE
           |─────────────┼─%${length}s─┼────────────────
           |      Status │ %${length}s │ Waiting
           |  SenderPath │ %${length}s │ %-${length}s
           | Delivery id │ %${length}s │ %-${length}s
           | Matches: $matches %s""".stripMargin,
        "VALUE",
        "─" * length,
        status,
        senderPath, destination,
        deliveryId.toString, expectedDeliveryId,
        if (recoveryRunning) "because recovery is running." else ""
      )
    })
    matches
  }
  /**
   * The behavior of this task. This is akin to the receive method of an actor, except for the fact that an
   * all catching pattern match will cause the orchestrator to fail. For example:
   * {{{
   *   def behavior = Receive {
   *     case m => //Some code
   *   }
   * }}}
   * Will cause the orchestrator to fail.
   */
  def behavior: Actor.Receive

  private def innerFinish(receivedMessage: Any, correlationId: CorrelationId)(extraActions: ⇒ Unit): Unit = {
    val deliveryId = getDeliveryId(correlationId)
    recoveryAwarePersist(MessageReceived(index, receivedMessage, correlationId, deliveryId)) {
      confirmDelivery(deliveryId)
      status = Finished
      extraActions
    }
  }

  /**
    * Signals that this task has finished.
    *
    * @param receivedMessage the received message which caused this task to finish.
    * @param correlationId the correlationId obtained from the message.
    */
  final def finish(receivedMessage: Any, correlationId: CorrelationId): Unit = ensureInStatus(Waiting, "Finish") {
    log.info(withLoggingPrefix(s"Finishing."))
    innerFinish(receivedMessage, correlationId) {
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
  final def terminateEarly(receivedMessage: Any, correlationId: CorrelationId): Unit = ensureInStatus(Waiting, "TerminateEarly") {
    log.info(withLoggingPrefix(s"Terminating Early."))
    innerFinish(receivedMessage, correlationId) {
      //This will prevent unstarted tasks from starting and onFinished from being called.
      earlyTerminated = true
      //This is invoked to remove this task behavior from the orchestrator
      updateCurrentBehavior()
      onEarlyTermination(this, receivedMessage, tasks.groupBy(_.status))
    }
  }

  /** @return whether this task status is Unstarted and all its dependencies have finished. */
  final def canStart: Boolean = status == Unstarted && dependencies.forall(_.hasFinished)
  /** @return whether this task status is `Waiting`. */
  final def isWaiting: Boolean = status == Waiting
  /** @return whether this task status is `Finished`. */
  final def hasFinished: Boolean = status == Finished

  /** The TaskStatus representation of this task. */
  final def toTaskStatus: TaskStatus = new TaskStatus(index, description, status, dependencies.map(_.index))

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
