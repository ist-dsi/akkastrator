package pt.ulisboa.tecnico.dsi.akkastrator

import scala.language.reflectiveCalls
import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.actor.ActorLogging
import akka.event.LoggingReceive
import akka.persistence._
import Message.Message

import scala.util.{Failure, Success}

object Orchestrator {
  case object StartReadyCommands

  case object SaveSnapshot
  case class Retry(commandIndex: Int)

  sealed trait Event
  case class MessageSent(commandIndex: Int, retryIteration: Int) extends Event
  case class ResponseReceived(response: Message, commandIndex: Int) extends Event

  trait State
  case object EmptyState extends State
}

/**
 * An Orchestrator executes an Action, which is a set of Tasks. A Command executes a Task.
 * A task corresponds to sending a message to an actor, handling its response and possibly
 * mutate the internal state of the Orchestrator.
 *
 * The Orchestrator together with the Command is able to:
 *
 *  - Handling the persistence of the internal state maintained by both the Orchestrator and the Commands.
 *  - Delivering messages with at-least-once delivery guarantee.
 *  - Handling Status messages, that is, if some actor is interested in querying the Orchestrator for its current
 *    status, the Orchestrator will respond with the status of each Task. The dependencies between tasks are not stated.
 *  - Performing business level retries. This is useful if one of the responses to a Task implies
 *    that the Task must be retried.
 *  - Commands that have no dependencies will be started right away and the Orchestrator will, from that point
 *    onwards, be prepared to handle the responses to the sent messages.
 *  - If the Orchestrator crashes, the state it maintains will be correctly restored.
 *
 * NOTE: the responses that are received must be Serializable.
 *
 * In order for the Orchestrator and the Commands to be able to achieve all of this they have to access and/or modify
 * each others state directly. This means they are very tightly coupled with each other.
 *
 * @param action the action message that prompted the construction of this orchestrator.
 *                Used to verify if this orchestrator should respond to Status messages.
 */
abstract class Orchestrator(action: Message) extends PersistentActor with ActorLogging with AtLeastOnceDelivery {
  import Orchestrator._
  //This exists to make the creation of Commands more simple.
  implicit val orchestrator = this

  log.info(s"Started for action message: $action")

  /**
   * The persistenceId used by the akka-persistence module.
   * The default value is this class simple name plus the id of `action`.
   */
  val persistenceId: String = this.getClass.getSimpleName + action.id.toString

  private var commands: IndexedSeq[Command] = Vector.empty
  private[akkastrator] def addCommand(command: Command): Int = {
    require(commands.contains(command) == false, "Command already exists. Commands must be unique.")
    val index = commands.length
    commands :+= command
    index
  }

  /** The state that this orchestrator maintains. */
  private[this] var _state: State = EmptyState
  def state[S <: State]: S = _state.asInstanceOf[S]
  def state_=[S <: State](state: S) = _state = state

  /** The interval at which snapshots will be saved. Use Duration.Zero to disable snapshots. */
  val saveSnapshotInterval: FiniteDuration

  /**
   * @return the behaviors of the commands which are waiting plus `orchestratorReceive`.
   */
  protected[akkastrator] def updateCurrentBehavior(): Unit = {
    val commandBehaviors = commands.filter(_.isWaiting).map(_.behavior)
    //Since orchestratorReceive always exists the reduce wont fail
    val newBehavior = (commandBehaviors :+ orchestratorReceive).reduce(_ orElse _)
    context.become(newBehavior)
  }

  private def orchestratorReceive: Receive = LoggingReceive {
    case StartReadyCommands =>
      commands.filter(_.canStart).foreach(_.start())
      if (commands.forall(_.hasFinished)) {
        onFinish()
        log.info(s"Finished!")
        context stop self
      }
    case Retry(commandIndex) =>
      commands(commandIndex).start()
    case StatusById(messageID, id) if messageID == action.id =>
      sender() ! Status(commands.map(_.toTask), id)
    case StatusByMessage(m, id) if m == action => //the == must ignore the id, otherwise this wont work
      sender() ! Status(commands.map(_.toTask), id)
    case SaveSnapshot =>
      saveSnapshot(_state)
  }

  /**
   * Once every command in the Orchestrator finishes this method is invoked.
   * The default implementation sends an `ActionFinished` to the Orchestrator parent and deletes all
   * persisted messages and snapshots. Simply override this method to obtain a different behavior.
   *
   * After this method executes the orchestrator is stopped.
   */
  def onFinish(): Unit = {
    context.parent ! ActionFinished(action.id)
    deleteMessages(lastSequenceNr + 1)
    deleteSnapshots(SnapshotSelectionCriteria())
    //Should we handle DeleteMessagesSuccess and DeleteSnapshotsSuccess?
  }

  final def receiveCommand: Receive = orchestratorReceive

  final def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot) =>
      state = offeredSnapshot.asInstanceOf[State]
    case MessageSent(commandIndex, retryIteration) =>
      val command = commands(commandIndex)
      log.info(s"${command.loggingPrefix} Recovering MessageSent")
      command.start(retryIteration)
    case ResponseReceived(_message, commandIndex) =>
      val command = commands(commandIndex)
      log.info(s"${command.loggingPrefix} Recovering ResponseReceived")
      command.behavior.apply(_message)
    case RecoveryCompleted =>
      //This gets us started
      self ! StartReadyCommands

      import context.dispatcher
      if (saveSnapshotInterval != Duration.Zero) {
        context.system.scheduler.schedule(saveSnapshotInterval, saveSnapshotInterval) {
          self ! SaveSnapshot
        }
      }
  }

  /*override def unhandled(message: Any): Unit = {
    log.error(s"""
         |Received unhandled message:
         |\tMessage: $message
         |\tSender: ${sender().path}
         |\tBehavior.isDefinedAt: ${receive.isDefinedAt(message)}
       """.stripMargin)
  }*/
}