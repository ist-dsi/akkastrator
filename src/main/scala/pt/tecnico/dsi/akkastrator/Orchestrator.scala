package pt.tecnico.dsi.akkastrator

import akka.actor.ActorLogging
import akka.event.LoggingReceive
import akka.persistence._

object Orchestrator {
  private[akkastrator] case object StartReadyTasks

  case object SaveSnapshot

  sealed trait Event
  case class MessageSent(taskIndex: Int) extends Event
  case class ResponseReceived(response: Any, taskIndex: Int) extends Event

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
  * In order for the Orchestrator and the Tasks to be able to achieve all of this they have to access and modify
  * each others state directly. This means they are very tightly coupled with each other. To make this relation more
  * obvious and to enforce it, you will only be able to create tasks inside an orchestrator.
  *
  * If you have the need to refactor the creation of tasks so that you can use them in multiple orchestrators you can
  * leverage self type annotations like so:
  * {{{
  *   trait DatabaseTasks { self: Orchestrator =>
  *     def createDoStuffTask(): Task = new Task("") {
  *       val destination: ActorPath = ???
  *       def createMessage(deliveryId: Long): Any = ???
  *
  *       def behavior: Receive = ???
  *     }
  *   }
  * }}}
  */
abstract class Orchestrator(val settings: Settings = new Settings()) extends PersistentActor with ActorLogging with AtLeastOnceDelivery {
  import Orchestrator._
  //This exists to make the creation of Tasks more simple.
  implicit val orchestrator = this

  log.info(s"Started Orchestrator ${this.getClass.getSimpleName}")

  /**
   * The persistenceId used by the akka-persistence module.
   * The default value is this class simple name.
   */
  val persistenceId: String = this.getClass.getSimpleName

  private var _tasks: IndexedSeq[Task] = Vector.empty
  private[akkastrator] def addTask(task: Task): Int = {
    val index = _tasks.length
    _tasks :+= task
    index
  }
  def tasks: IndexedSeq[Task] = _tasks

  private[akkastrator] var counter = 0

  /** The state that this orchestrator maintains. */
  private[this] var _state: State = EmptyState
  def state[S <: State]: S = _state.asInstanceOf[S]
  def state_=[S <: State](state: S): Unit = _state = state

  /**
    * Every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * By default this method returns the value defined in the configuration.
    * You can trigger a save snapshot manually by sending a SaveSnapshot message to this orchestrator.
    */
  def saveSnapshotEveryXMessages: Int = settings.saveSnapshotEveryXMessages

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
        onFinish()
      }
    case Status(id) =>
      sender() ! StatusResponse(tasks.map(_.toTaskStatus), id)
    case SaveSnapshot =>
      saveSnapshot(_state)
  }

  /**
    * This method is invoked once every task finishes.
    * The default implementation just stops the orchestrator.
    *
    * You can use this to implement your termination strategy.
    */
  def onFinish(): Unit = {
    context stop self
  }

  /**
    * This method is invoked once recovery completes.
    *
    * You can use this to perform additional initialization after the recovery has completed but before any
    * other message sent to the orchestrator is processed.
    */
  def onRecoveryCompleted(): Unit = ()

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
      onRecoveryCompleted()
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