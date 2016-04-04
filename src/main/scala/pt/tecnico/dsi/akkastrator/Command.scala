package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.ActorPath
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
  val loggingPrefix = s"[$index - $description]"

  /**
   * The ActorPath to whom this Command will send the message(s).
   */
  val destination: ActorPath
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
   * Effectively sends a message to destination.
   * Updates status to Waiting.
   * Updates the Orchestrator behavior to include this Command behavior.
   */
  final private def deliverMessage(retryIteration: Int): Unit = {
    orchestrator.deliver(destination) { deliveryId =>
      _status = Waiting(deliveryId, retryIteration)
      orchestrator.log.info(s"$loggingPrefix Waiting(messageID=$deliveryId, retryIteration=${status.retryIteration})")
      orchestrator.updateCurrentBehavior()
      createMessage(deliveryId)
    }
  }
  /**
   * Starts the execution of this Command task.
   * A Command can only be started if its state is either Unstarted or WaitingToRetry.
   * Any other state will cause an error to be logged.
   *
   * We first persist that the message was sent, then we send it.
   * If the Orchestrator is recovering then we just send the message because there is
   * no need to persist that the message was sent.
   */
  final protected[akkastrator] def start(retryIteration: Int = status.retryIteration): Unit = status match {
    case Unstarted | _: WaitingToRetry =>
      orchestrator.log.info(s"$loggingPrefix Starting.")
      if (orchestrator.recoveryRunning) {
        //When we are recovering we just deliver the message.
        //We do not want to persist again that the message was sent.
        deliverMessage(retryIteration)
      } else {
        orchestrator.persist(MessageSent(index, retryIteration)) { _ =>
          deliverMessage(retryIteration)
        }
      }
    case _ =>
      orchestrator.log.error(s"""$loggingPrefix Start was invoked erroneously:
                                |Status = $status""".stripMargin)
  }

  /**
   * @param receivedMessage the received message.
   * @return true if the id of `receivedMessage` is the same as the one in the `Waiting` status and
   * the sender path is the same path as `destination`. False otherwise. If the current state of this command is not
   * waiting false will be returned.
   * @note This check is necessary because the orchestrator is ready to receive the responses to multiple commands
   *       at the same time. And all of these responses might have the same id.
   */
  final def matchSenderAndId(receivedMessage: Message): Boolean = {
    orchestrator.log.info(s"$loggingPrefix MatchSenderAndId with receivedMessage: " + receivedMessage.getClass().getName)
    status match {
      case Waiting(id, _) => receivedMessage.id == id && orchestrator.sender().path == destination
      case _ => false
    }
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
   * Re-sends the message.
   * A Command can only be retried if its state is Waiting. Any other state will cause an error to be logged.
   *
   * Retries are attempted in a exponential backoff manner.
   * @param receivedMessage
   */
  final def retry(receivedMessage: Message): Unit = status match {
    case Waiting(id, _) if matchSenderAndId(receivedMessage) =>
      orchestrator.log.debug(s"$loggingPrefix Retrying.")
      //We must guarantee that confirmDelivery is invoked for the previous message
      orchestrator.persist(ResponseReceived(receivedMessage, index)) { case ResponseReceived(m, _) =>
        orchestrator.confirmDelivery(m.id)
      }

      //TODO: reimplement this functionality val delay = Backoff.exponentialCapped(status.retryIteration)
      val delay = backoff(status.retryIteration)
      import orchestrator.context.dispatcher
      orchestrator.context.system.scheduler.scheduleOnce(delay) {
        orchestrator.self ! Retry(index)
      }
      _status = WaitingToRetry(status.retryIteration + 1)
      orchestrator.log.info(s"$loggingPrefix WaitingToRetry(messageID=$id, retryIteration=${status.retryIteration})")
    case _ =>
      orchestrator.log.error(s"""$loggingPrefix Retry was invoked erroneously:
                   |\tMessage = $receivedMessage
                   |\tStatus = $status""".stripMargin)
  }

  /**
   * Confirms the message with id `deliveryId` was delivered.
   * Asks the orchestrator to start commands that are able to start.
   * Updates the Orchestrator behavior to include this Command behavior.
   */
  final private def finishMessage(deliveryId: Long): Unit = {
    orchestrator.confirmDelivery(deliveryId)

    _status = Finished(deliveryId, status.retryIteration)
    orchestrator.log.info(s"$loggingPrefix Finished(messageID=$deliveryId, retryIteration=${status.retryIteration})")
    //This starts commands that have a dependency on this command
    orchestrator.self ! StartReadyCommands
    orchestrator.updateCurrentBehavior()
  }
  final def finish(receivedMessage: Message): Unit = status match {
    case Finished(id, _) if matchSenderAndId(receivedMessage) =>
      //We already received a response for this command and handled it.
      //Which must mean the receivedMessage is a response to a resend.
      //We can safely ignore it.
      orchestrator.log.debug(s"""[$description]
                   |Finish was invoked when status is already Finished (this might be a resend):
                   |\tMessage = $receivedMessage
                   |\tStatus = $status""".stripMargin)
    case Waiting(id, _) if matchSenderAndId(receivedMessage) =>
      orchestrator.log.info(s"$loggingPrefix Finishing.")
      if (orchestrator.recoveryRunning) {
        //When we are recovering we do not want to persist again that the message was received.
        //So we just confirm the delivery of the message again (the finishMessage invokes the confirmDelivery).
        finishMessage(receivedMessage.id)
      } else {
        orchestrator.persist(ResponseReceived(receivedMessage, index)) { case ResponseReceived(m, _) =>
          finishMessage(m.id)
        }
      }
    case _ =>
      orchestrator.log.error(s"""$loggingPrefix Finish was invoked erroneously:
                   |\tMessage = $receivedMessage
                   |\tSender = ${orchestrator.sender().path}
                   |\tDestination = $destination
                   |\tStatus = $status""".stripMargin)
  }

  /**
   * @return whether this command is able to start. A command is ready to start if its status is Unstarted and all its
   *         dependencies have finished.
   */
  final def canStart: Boolean = dependencies.forall(_.hasFinished) && status == Unstarted
  /**
   * @return whether this command is waiting for a response. A command is waiting if a message has been
   *         sent to `destination` and the finish method hasn't been invoked.
   */
  final def isWaiting: Boolean = status.isInstanceOf[Waiting]
  /**
   * @return whether this command has finished. A command has finished when the `finish` method is invoked.
   */
  final def hasFinished: Boolean = status.isInstanceOf[Finished]
  /**
   * @return the current status of the command.
   */
  final def status: Task.Status = _status

  /**
   * @return The Task representation of this Command.
   */
  final def toTask: Task = Task(description, status)

  final override def equals(other: Any): Boolean = other match {
    case that: Command =>
      description == that.description &&
        dependencies == that.dependencies
    case _ => false
  }
  final override def hashCode(): Int = {
    val state = Seq(description, dependencies)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Command($description, $destination, $status)"
}
