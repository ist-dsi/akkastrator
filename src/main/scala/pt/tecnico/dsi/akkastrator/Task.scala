package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.ActorPath
import pt.tecnico.dsi.akkastrator.Orchestrator.{MessageSent, ResponseReceived, SaveSnapshot, StartReadyTasks}
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

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
 * The pattern matching inside `behavior` should invoke `matchSenderAndID` to ensure
 * the received message is in fact the one that we were waiting to receive.
 * The internal state of the orchestrator might be mutated inside `behavior`.
 *
 * This class is very tightly coupled with Orchestrator and the reverse is also true.
 *
 * This class changes the internal state of the orchestrator:
 *
 *  - The task constructor adds the constructed task to the `tasks` variable.
 *  - Inside `behavior` the internal state of the orchestrator may be changed.
 *  - If all tasks are finished the orchestrator will be stopped.
 *  - Otherwise, the next behavior of the orchestrator will be computed.
 *
 * In exchange the orchestrator changes the internal state of the task:
 *
 *  - By invoking `start` which changes the task status to Waiting.
 */
abstract class Task(val description: String, val dependencies: Set[Task] = Set.empty[Task])
                   (private implicit val orchestrator: Orchestrator) {
  val index = orchestrator.addTask(this)

  private val colors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.BLACK,
    Console.RED,
    Console.WHITE
  )

  private val color = colors(index % colors.size)

  def withLoggingPrefix(message: String): String = f"$color[$index%02d - $description] $message${Console.RESET}"

  //The task always starts in the Unstarted status and without an expectedDeliveryId
  status = Unstarted
  expectedDeliveryId = None

  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change
  /** The constructor of the message to be sent. */
  def createMessage(deliveryId: Long): Any

  /**
   * Starts the execution of this task.
   * If this task is already Waiting or Finished an error will be logged.
   *
   * We first persist that the message was sent, then we send it.
   * If the Orchestrator is recovering then we just send the message because there is
   * no need to persist that the message was sent.
   */
  final protected[akkastrator] def start(): Unit = status match {
    case Unstarted =>
      orchestrator.log.info(withLoggingPrefix(s"Sending message."))

      def deliverMessage(): Unit = {
        orchestrator.log.debug(withLoggingPrefix(s"Deliver message."))
        orchestrator.deliver(destination) { deliveryId =>
          expectedDeliveryId = Some(deliveryId)
          createMessage(deliveryId)
        }
        status = Waiting
        orchestrator.updateCurrentBehavior()
      }

      if (orchestrator.recoveryRunning) {
        //When we are recovering we do not want to persist again that the message was sent.
        //We just want to deliver the message again.
        deliverMessage()
      } else {
        orchestrator.log.debug(withLoggingPrefix(s"Persisting MessageSent."))
        orchestrator.persist(MessageSent(index)) { _ =>
          orchestrator.log.debug(withLoggingPrefix(s"Successful persist."))
          deliverMessage()
        }
      }
    case _ => orchestrator.log.error(withLoggingPrefix(s"Start was invoked erroneously (while in status $status)."))
  }

  /**
   * @param deliverId the deliveryId obtained from the received message.
   * @return true if the deliveryId of `receivedMessage` is the same as the one this task is expecting. False otherwise.
   */
  final def matchDeliveryId(deliverId: Long): Boolean = {
    val matches = status == Waiting && expectedDeliveryId.contains(deliverId)
    orchestrator.log.debug(withLoggingPrefix(
      s"""MatchSenderAndId:
         |Status: $status
         |Expected Delivery id: $expectedDeliveryId
         |MATCHES = $matches""".stripMargin))
    matches
  }
  /**
   * The behavior of this task. This is akin to the receive method of an actor, except for the fact that an
   * all catching pattern match will cause the orchestrator to fail. For example:
   * {{{
   *   def behavior = Receive {
   *     case m if matchDeliveryId(m) => //Some code
   *   }
   * }}}
   * Will cause the orchestrator to fail.
   */
  def behavior: Receive

  /**
    * Signals that this task has finished.
    *
    * @param receivedMessage the received message that signaled this task has finished.
    */
  final def finish(receivedMessage: Any, deliveryId: Long): Unit = status match {
    case Waiting if matchDeliveryId(deliveryId) =>
      orchestrator.log.info(withLoggingPrefix(s"Finishing (received response)."))

      def finishMessage(): Unit = {
        orchestrator.confirmDelivery(deliveryId)
        status = Finished

        if (orchestrator.saveSnapshotEveryXMessages > 0) {
          orchestrator.counter += 1
          if (orchestrator.counter >= orchestrator.saveSnapshotEveryXMessages) {
            orchestrator.self ! SaveSnapshot
          }
        }

        //This starts tasks that have a dependency on this task
        //And also removes this task behavior from the orchestrator behavior
        orchestrator.self ! StartReadyTasks
      }

      if (orchestrator.recoveryRunning) {
        //When we are recovering we do not want to persist again that the message was received.
        //We just want to confirm the delivery of the message again.
        finishMessage()
      } else {
        orchestrator.log.debug(withLoggingPrefix(s"Persisting MessageReceived."))
        orchestrator.persist(ResponseReceived(receivedMessage, index)) { _ =>
          orchestrator.log.debug(withLoggingPrefix(s"Successful persist."))
          finishMessage()
        }
      }
    case Finished if matchDeliveryId(deliveryId) =>
      //We already received a response for this task and handled it.
      //Which must mean the receivedMessage is a response to a resend.
      //We can safely ignore it.
      orchestrator.log.debug(withLoggingPrefix(s"Finish was invoked when status is already Finished. Ignoring the resent message."))
    case _ =>
      orchestrator.log.error(withLoggingPrefix(s"""Finish was invoked erroneously:
                   |\tMessage = $receivedMessage
                   |\tSender = ${orchestrator.sender().path}
                   |\tDestination = $destination
                   |\tStatus = $status""".stripMargin))
  }

  /** @return whether this task status is Unstarted and all its dependencies have finished. */
  final def canStart: Boolean = dependencies.forall(_.hasFinished) && status == Unstarted
  /** @return whether this task status is `Waiting`. */
  final def isWaiting: Boolean = status == Waiting
  /** @return whether this task status is `Finished`. */
  final def hasFinished: Boolean = status == Finished

  /** The TaskStatus representation of this task. */
  final def toTaskStatus: TaskStatus = new TaskStatus(index, description, status, dependencies.map(_.index))

  private var _status: Task.Status = _
  /** @return the current status of this Task. */
  final def status: Task.Status = _status
  private def status_=(status: Task.Status): Unit = {
    _status = status
    orchestrator.log.info(withLoggingPrefix(s"Status: $status."))
  }

  private var _expectedDeliveryId: Option[Long] = _
  /** @return the current expected deliveryId of this Task. */
  final def expectedDeliveryId: Option[Long] = _expectedDeliveryId
  private def expectedDeliveryId_=(expectedDeliveryId: Option[Long]): Unit = {
    _expectedDeliveryId = expectedDeliveryId
  }

  override def toString: String =
    s"""Task [$index%02d - $description]:
       |Destination: $destination
       |Status: $status"""".stripMargin
}
