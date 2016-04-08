package pt.tecnico.dsi.akkastrator

import akka.actor.ActorLogging
import akka.event.LoggingReceive
import akka.persistence._
import pt.tecnico.dsi.Backoff
import pt.tecnico.dsi.akkastrator.Message.Message

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.reflectiveCalls

object Orchestrator {
  private[akkastrator] case object StartReadyCommands

  private case object SaveSnapshot
  private[akkastrator] case class Retry(commandIndex: Int)

  private[akkastrator] sealed trait Event
  private[akkastrator] case class MessageSent(commandIndex: Int) extends Event
  private[akkastrator] case class ResponseReceived(response: Message, commandIndex: Int) extends Event

  trait State
  case object EmptyState extends State
}

/**
 * An Orchestrator executes a set of, possibly dependent, tasks. Each individual task is executed by a Command.
 * A task corresponds to sending a message to an actor, handling its response and possibly
 * mutate the internal state of the Orchestrator.
 *
 * The Orchestrator together with the Command is able to:
 *
 *  - Handling the persistence of the internal state maintained by both the Orchestrator and the Commands.
 *  - Delivering messages with at-least-once delivery guarantee.
 *  - Handling Status messages, that is, if some actor is interested in querying the Orchestrator for its current
 *    status, the Orchestrator will respond with the status of each task. The dependencies between tasks are not stated.
 *  - Performing business level retries. This is useful if one of the responses to a task implies
 *    that the task must be retried.
 *  - Commands that have no dependencies will be started right away and the Orchestrator will, from that point
 *    onwards, be prepared to handle the responses to the sent messages.
 *  - If the Orchestrator crashes, the state it maintains will be correctly restored.
 *
 * NOTE: the responses that are received must be Serializable.
 *
 * In order for the Orchestrator and the Commands to be able to achieve all of this they have to access and/or modify
 * each others state directly. This means they are very tightly coupled with each other.
 */
trait Orchestrator extends PersistentActor with ActorLogging with AtLeastOnceDelivery {
  import Orchestrator._
  //This exists to make the creation of Commands more simple.
  implicit val orchestrator = this

  log.info(s"Started Orchestrator ${this.getClass.getSimpleName}")

  /**
   * The persistenceId used by the akka-persistence module.
   * The default value is this class simple name.
   */
  val persistenceId: String = this.getClass.getSimpleName

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
  def state_=[S <: State](state: S): Unit = _state = state

  /** The interval at which snapshots will be saved. Use Duration.Zero to disable snapshots. */
  val saveSnapshotInterval: FiniteDuration

  /**
    * How long to wait when performing a business retry.
    *
    * Be default this is an exponential backoff where each iteration will wait the double
    * amount of time of the last iteration.
    *
    * Simply override this method to obtain a different behavior.
    *
    * By default the backoff of every command in this orchestrator simply calls this one.
    * You can specify a different backoff for a specific command by overriding its backoff function.
    *
    * @param iteration how many times was the message sent
    * @return how long to wait
    */
  def backoff(iteration: Int): FiniteDuration = {
    require(iteration >= 0, "Iteration must be positive.")
    Backoff.exponential(iteration)
  }

  //TODO: If an orchestrator has more than on command talking with the same actor simultaneously the method matchSenderAndId
  //TODO: wont be sufficient to disambiguate for which command the message was destined, specially if both messages are of
  //TODO: the same type and the commands are all waiting for that message type.

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
        log.info(s"Orchestrator Finished!")
        context stop self
      }
    case Retry(commandIndex) =>
      commands(commandIndex).start()
    case Status(id) =>
      sender() ! StatusResponse(commands.map(_.toTask), id)
    case SaveSnapshot =>
      saveSnapshot(_state)
  }

  final def receiveCommand: Receive = orchestratorReceive

  final def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot) =>
      state = offeredSnapshot.asInstanceOf[State]
    case MessageSent(commandIndex) =>
      val command = commands(commandIndex)
      log.info(command.withLoggingPrefix("Recovering MessageSent"))
      command.start()
    case ResponseReceived(_message, commandIndex) =>
      val command = commands(commandIndex)
      log.info(command.withLoggingPrefix("Recovering ResponseReceived"))
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