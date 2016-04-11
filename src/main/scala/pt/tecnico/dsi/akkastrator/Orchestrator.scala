package pt.tecnico.dsi.akkastrator

import akka.actor.ActorLogging
import akka.event.LoggingReceive
import akka.persistence._

import scala.concurrent.duration.{Duration, FiniteDuration}

object Orchestrator {
  private[akkastrator] case object StartReadyTasks

  private case object SaveSnapshot

  private[akkastrator] sealed trait Event
  private[akkastrator] case class MessageSent(taskIndex: Int) extends Event
  private[akkastrator] case class ResponseReceived(response: Any, taskIndex: Int) extends Event

  trait State
  case object EmptyState extends State
}

/**
 * An Orchestrator executes a set of, possibly dependent, `Task`s.
 * A task corresponds to sending a message to an actor, handling its response and possibly
 * mutate the internal state of the Orchestrator.
 *
 * The Orchestrator together with the Task is able to:
 *
 *  - Handling the persistence of the internal state maintained by both the Orchestrator and the Tasks.
 *  - Delivering messages with at-least-once delivery guarantee.
 *  - Handling Status messages, that is, if some actor is interested in querying the Orchestrator for its current
 *    status, the Orchestrator will respond with the status of each task.
 *  - Tasks that have no dependencies will be started right away and the Orchestrator will, from that point
 *    onwards, be prepared to handle the responses to the sent messages.
 *  - If the Orchestrator crashes, the state it maintains will be correctly restored.
 *
 * NOTE: the responses that are received must be Serializable.
 *
 * In order for the Orchestrator and the Tasks to be able to achieve all of this they have to access and/or modify
 * each others state directly. This means they are very tightly coupled with each other.
 */
trait Orchestrator extends PersistentActor with ActorLogging with AtLeastOnceDelivery {
  import Orchestrator._
  //This exists to make the creation of Tasks more simple.
  implicit val orchestrator = this

  log.info(s"Started Orchestrator ${this.getClass.getSimpleName}")

  /**
   * The persistenceId used by the akka-persistence module.
   * The default value is this class simple name.
   */
  val persistenceId: String = this.getClass.getSimpleName

  private var tasks: IndexedSeq[Task] = Vector.empty
  private[akkastrator] def addTask(task: Task): Int = {
    val index = tasks.length
    tasks :+= task
    index
  }

  /** The state that this orchestrator maintains. */
  private[this] var _state: State = EmptyState
  def state[S <: State]: S = _state.asInstanceOf[S]
  def state_=[S <: State](state: S): Unit = _state = state

  /** The interval at which snapshots will be saved. Use Duration.Zero to disable snapshots. */
  val saveSnapshotInterval: FiniteDuration

  /**
   * @return the behaviors of the tasks which are waiting plus `orchestratorReceive`.
   */
  protected[akkastrator] def updateCurrentBehavior(): Unit = {
    val taskBehaviors = tasks.filter(_.isWaiting).map(_.behavior)
    //Since orchestratorReceive always exists the reduce wont fail
    val newBehavior = (taskBehaviors :+ orchestratorReceive).reduce(_ orElse _)
    context.become(newBehavior)
  }

  private def orchestratorReceive: Receive = LoggingReceive {
    case StartReadyTasks =>
      tasks.filter(_.canStart).foreach(_.start())
      if (tasks.forall(_.hasFinished)) {
        log.info(s"Orchestrator Finished!")
        context stop self
      }
    case Status(id) =>
      sender() ! StatusResponse(tasks.map(_.toTaskStatus), id)
    case SaveSnapshot =>
      saveSnapshot(_state)
  }

  final def receiveCommand: Receive = orchestratorReceive

  final def receiveRecover: Receive = {
    case SnapshotOffer(metadata, offeredSnapshot) =>
      state = offeredSnapshot.asInstanceOf[State]
    case MessageSent(taskIndex) =>
      val task = tasks(taskIndex)
      log.info(task.withLoggingPrefix("Recovering MessageSent"))
      task.start()
    case ResponseReceived(message, taskIndex) =>
      val task = tasks(taskIndex)
      log.info(task.withLoggingPrefix("Recovering ResponseReceived"))
      task.behavior.apply(message)
    case RecoveryCompleted =>
      //This gets us started
      self ! StartReadyTasks

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