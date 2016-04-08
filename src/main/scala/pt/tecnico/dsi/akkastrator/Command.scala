package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.ActorPath
import akka.event.Logging.MDC
import pt.tecnico.dsi.akkastrator.Message.{Message, MessageId}
import pt.tecnico.dsi.akkastrator.Orchestrator.{MessageSent, ResponseReceived, Retry, StartReadyCommands}
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting, WaitingToRetry}

import scala.concurrent.duration.FiniteDuration
import scala.language.reflectiveCalls

/**
 * A Command executes a Task. A task corresponds to sending a message to an actor, handling its response and possibly
 * mutate the internal state of the Orchestrator.
 * The answer(s) to the sent message must be handled in `behavior`.
 * `behavior` must invoke `finish` when no further processing is necessary.
 * The pattern matching inside `behavior` should invoke `matchSenderAndID` to ensure
 * the received message is in fact the one that we were waiting to receive.
 * The internal state of the orchestrator might be mutated inside `behavior`.
 * If a business resend is necessary `retry` should be invoked inside `behavior`.
 *
 * This class is super tightly coupled with Orchestrator and the reverse is also true.
 *
 * This class changes the internal state of the orchestrator:
 *
 *  - The command constructor adds the constructed command to the `commands` variable.
 *  - Inside the `behavior` of the command the internal state of the orchestrator may be changed.
 *  - If all commands are finished the `finish`(of the orchestrator) will stop the orchestrator.
 *  - Otherwise, the next behavior of the orchestrator will be computed.
 *
 * In exchange the orchestrator changes the internal state of the Command:
 *
 *  - By invoking `start` which changes the command status to Waiting.
 */
abstract class Command(val description: String, val dependencies: Set[Command] = Set.empty[Command])
                      (implicit val orchestrator: Orchestrator) {
  private val index = orchestrator.addCommand(this)
  private var _status: Task.Status = Unstarted
  private var expectedMessageId: MessageId = Long.MinValue
  private var _numberOfRetries: Int = 0

  def withLoggingPrefix(message: String): String = {
    val color = index % 8 match {
      case 0 => Console.MAGENTA
      case 1 => Console.CYAN
      case 2 => Console.GREEN
      case 3 => Console.BLUE
      case 4 => Console.YELLOW
      case 5 => Console.BLACK
      case 6 => Console.RED
      case 7 => Console.WHITE
    }
    f"$color[$index%02d - $description] $message${Console.RESET}"
  }

  private def logStatus(): Unit = orchestrator.log.info(withLoggingPrefix(s"Status: $status."))
  logStatus()

  /**
   * The ActorPath to whom this Command will send the message(s).
   */
  val destination: ActorPath //This must be a val because the destination cannot change
  /**
   * The "constructor" of the message(s) to be sent.
   * There is the need to have a "constructor" because we want to send messages with at-least-once semantics.
   * Which means we might have to send more than one message. These messages will only differ on the messageID.
   */
  def createMessage(id: MessageId): Message

  /**
    * How long to wait when performing a business resend.
    *
    * Be default this function just calls the same function of orchestrator, thus "inheriting" its backoff function.
    *
    * You can specify a different backoff for this command by overriding this function.
    *
    * @param iteration how many times was the message sent
    * @return how long to wait
    */
  def backoff(iteration: Int): FiniteDuration = orchestrator.backoff(iteration)

  /**
   * Starts the execution of this Command.
   * A Command can only be started if its either Unstarted or WaitingToRetry.
   * If the command is already Waiting or Finished an error will be logged.
   *
   * We first persist that the message was sent, then we send it.
   * If the Orchestrator is recovering then we just send the message because there is
   * no need to persist that the message was sent.
   */
  final protected[akkastrator] def start(): Unit = status match {
    case Unstarted | WaitingToRetry =>
      orchestrator.log.info(withLoggingPrefix(s"Sending message."))

      def deliverMessage(): Unit = {
        orchestrator.log.debug(withLoggingPrefix(s"Deliver message."))
        orchestrator.deliver(destination) { deliveryId =>
          expectedMessageId = deliveryId
          createMessage(deliveryId)
        }
        _status = Waiting
        logStatus()
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
   * @param receivedMessage the received message.
   * @return true if the id of `receivedMessage` is the same as the one this command is expecting and
   * the sender path is the same path as `destination`. False otherwise.
   * @note This check is necessary because the orchestrator is ready to receive the responses to multiple commands
   *       at the same time. And all of these responses might have the same id.
   */
  final def matchSenderAndId(receivedMessage: Message): Boolean = {
    val matches = status == Waiting && receivedMessage.id == expectedMessageId && orchestrator.sender().path == destination
    orchestrator.log.debug(withLoggingPrefix(
      s"""MatchSenderAndId:
         |Status: $status
         |Received message name: ${receivedMessage.getClass().getName}
         |Received message id: ${receivedMessage.id}
         |Expected message id: $expectedMessageId
         |Sender path: ${orchestrator.sender().path}
         |Destination: $destination
         |MATCHES = $matches""".stripMargin))
    matches
  }
  /**
   * The behavior of this command. This is akin to the receive method of an actor, except for the fact that an
   * all catching pattern match will cause the orchestrator to fail. For example:
   * {{{
   *   def behavior = Receive {
   *     case m if matchSenderAndId(m) => //Some code
   *   }
   * }}}
   * Will cause the orchestrator to fail.
   */
  def behavior: Receive

  /**
   * Performs a business retry aka re-sends the message.
   * A Command can only be retried if its state is Waiting. Any other state will cause an error to be logged.
   *
   * Retries are attempted with a backoff in other to not overload the destination.
   */
  final def retry(receivedMessage: Message): Unit = status match {
    case Waiting if matchSenderAndId(receivedMessage) =>
      orchestrator.log.info(withLoggingPrefix(s"Retrying."))
      if (orchestrator.recoveryRunning == false) {
        //We must guarantee that confirmDelivery is invoked for the previous message
        orchestrator.log.debug(withLoggingPrefix(s"Persisting MessageReceived."))
        orchestrator.persist(ResponseReceived(receivedMessage, index)) { _ =>
          orchestrator.log.debug(withLoggingPrefix(s"Successful persist."))
          orchestrator.confirmDelivery(receivedMessage.id)
        }
      }

      import orchestrator.context.dispatcher
      orchestrator.context.system.scheduler.scheduleOnce(delay = backoff(_numberOfRetries)) {
        orchestrator.self ! Retry(index)
      }
      _status = WaitingToRetry
      _numberOfRetries += 1
      logStatus()
    case _ =>
      orchestrator.log.error(withLoggingPrefix(s"""Retry was invoked erroneously:
                   |\tMessage = $receivedMessage
                   |\tStatus = $status""".stripMargin))
  }

  /**
    * Signals that the task being performed by this command has finished.
    *
    * @param receivedMessage the received message that signaled that the task has finished.
    */
  final def finish(receivedMessage: Message): Unit = status match {
    case Waiting if matchSenderAndId(receivedMessage) =>
      orchestrator.log.info(withLoggingPrefix(s"Finishing (received response)."))

      def finishMessage(deliveryId: Long): Unit = {
        orchestrator.confirmDelivery(deliveryId)
        _status = Finished
        //expectedMessageId = Long.MinValue
        logStatus()
        //This starts commands that have a dependency on this command
        //And also removes this command behavior from the orchestrator behavior
        orchestrator.self ! StartReadyCommands
      }

      if (orchestrator.recoveryRunning) {
        //When we are recovering we do not want to persist again that the message was received.
        //We just want to confirm the delivery of the message again.
        finishMessage(receivedMessage.id)
      } else {
        orchestrator.log.debug(withLoggingPrefix(s"Persisting MessageReceived."))
        orchestrator.persist(ResponseReceived(receivedMessage, index)) { _ =>
          orchestrator.log.debug(withLoggingPrefix(s"Successful persist."))
          finishMessage(receivedMessage.id)
        }
      }
    case Finished if matchSenderAndId(receivedMessage) =>
      //We already received a response for this command and handled it.
      //Which must mean the receivedMessage is a response to a resend.
      //We can safely ignore it.
      orchestrator.log.debug(withLoggingPrefix(s"Finish was invoked when status is already Finished. This IS a resend."))
    case _ =>
      orchestrator.log.error(withLoggingPrefix(s"""Finish was invoked erroneously:
                   |\tMessage = $receivedMessage
                   |\tSender = ${orchestrator.sender().path}
                   |\tDestination = $destination
                   |\tStatus = $status""".stripMargin))
  }

  /**
   * @return whether this command is able to start. A command is ready to start if its
    *        status is Unstarted and all its dependencies have finished.
   */
  final def canStart: Boolean = dependencies.forall(_.hasFinished) && status == Unstarted
  /**
   * @return whether this command is waiting for a response. A command is waiting if a message has been
   *         sent to `destination` and the finish method hasn't been invoked.
   */
  final def isWaiting: Boolean = status == Waiting
  /**
   * @return whether this command has finished. A command has finished when the `finish` method is invoked.
   */
  final def hasFinished: Boolean = status == Finished
  /**
   * @return the current status of the command.
   */
  final def status: Task.Status = _status

  /**
    * @return how many business retries have been performed.
    */
  final def numberOfRetries: Int = _numberOfRetries

  /**
   * @return The Task representation of this Command.
   */
  final def toTask: Task = Task(description, status, _numberOfRetries)

  final override def equals(other: Any): Boolean = other match {
    case that: Command =>
      description == that.description && dependencies == that.dependencies
    case _ => false
  }
  final override def hashCode(): Int = {
    val state = Seq(description, dependencies)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String =
    s"""Command:
       |Description: $description
       |Destination: $destination
       |Status: $status
       |Number of retries: $numberOfRetries"""".stripMargin
}
