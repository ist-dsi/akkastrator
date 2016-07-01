package pt.tecnico.dsi.akkastrator

import akka.actor.{ActorLogging, ActorPath}
import akka.event.LoggingReceive
import akka.persistence._

import scala.collection.immutable.SortedMap

object Orchestrator {
  type DeliveryId = Long
  type CorrelationId = Long

  case object StartReadyTasks
  case object SaveSnapshot

  sealed trait Event
  case class MessageSent(taskIndex: Int) extends Event
  case class MessageReceived(taskIndex: Int, response: Any, correlationId: CorrelationId, deliveryId: DeliveryId) extends Event

  trait State {
    //By using a SortedMap as opposed to a Map we can also extract the latest correlationId per sender
    val idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]

    def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): State

    def getIdsFor(destination: ActorPath): SortedMap[CorrelationId, DeliveryId] = {
      idsPerDestination.getOrElse(destination, SortedMap.empty)
    }
    def updatedIdsPerDestination(destination: ActorPath, newIdRelation: (CorrelationId, DeliveryId)): State = {
      withIdsPerDestination(idsPerDestination.updated(destination, getIdsFor(destination) + newIdRelation))
    }
  }
  class MinimalState(val idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]] = Map.empty) extends State {

    def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): State = {
      new MinimalState(newIdsPerDestination)
    }
  }
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
  *     def createQueryTask(): Task = new Task("") {
  *       val destination: ActorPath = ???
  *       def createMessage(correlationId: CorrelationId): Any = ???
  *
  *       def behavior: Receive = ???
  *     }
  *   }
  * }}}
  */
abstract class Orchestrator(val settings: Settings = new Settings()) extends PersistentActor with AtLeastOnceDelivery with ActorLogging {
  import Orchestrator._
  //This exists to make the creation of Tasks more simple.
  implicit final val orchestrator = this

  log.info(s"Started ${this.getClass.getSimpleName} with PersistenceId: $persistenceId")

  private[akkastrator] var earlyTerminated = false

  private var _tasks: IndexedSeq[Task] = Vector.empty
  private[akkastrator] def addTask(task: Task): Int = {
    val index = _tasks.length
    _tasks :+= task
    index
  }
  def tasks: IndexedSeq[Task] = _tasks

  /** The state that this orchestrator maintains. */
  private[this] var _state: State = new MinimalState()
  def state[S <: State]: S = _state.asInstanceOf[S]
  def state_=[S <: State](state: S): Unit = _state = state

  /**
    * Every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * By default this method returns the value defined in the configuration.
    *
    * This value is not a strict one because the orchestrator will not save it in snapshots.
    * This means that when this actor crashes more messages than the ones defined here might have to
    * be processed in other to trigger a save snapshot.
    *
    * You can trigger a save snapshot manually by sending a `SaveSnapshot` message to this orchestrator.
    */
  def saveSnapshotEveryXMessages: Int = settings.saveSnapshotEveryXMessages
  /** How many messages have been processed. */
  private var counter = 0
  private[akkastrator] def incrementCounter(): Unit = {
    if (saveSnapshotEveryXMessages > 0) {
      counter += 1
      if (counter >= saveSnapshotEveryXMessages) {
        self ! SaveSnapshot
      }
    }
  }

  /**
    * Override this method to perform additional initialization after recovery has completed but before the
    * orchestrator starts.
    *
    * When you finish performing the additional initialization you MUST send the message `StartReadyTasks`
    * to the orchestrator which is what prompts the tasks execution. The default implementation of
    * this method does just this.
    * */
  def onRecoveryComplete(): Unit = {
    //This gets us started
    self ! StartReadyTasks
  }

  /**
    * This method is invoked once every task finishes.
    * The default implementation just stops the orchestrator and logs that the Orchestrator has finished.
    *
    * You can use this to implement your termination strategy.
    */
  def onFinish(): Unit = {
    log.info(s"Orchestrator Finished!")
    context stop self
  }

  /**
    * @param instigator the task that initiated the early termination.
    * @param causeMessage the message that caused the early termination.
    * @param tasks Map with the tasks status at the moment of early termination.
    */
  def onEarlyTermination(instigator: Task, causeMessage: Any, tasks: Map[Task.Status, Seq[Task]]): Unit = ()

  /** @return the behaviors of the tasks which are waiting plus `orchestratorReceiveCommand`. */
  private[akkastrator] final def updateCurrentBehavior(): Unit = {
    val taskBehaviors = tasks.filter(_.isWaiting).map(_.behavior)
    //Since orchestratorReceiveCommand always exists the reduce wont fail
    val newBehavior = (taskBehaviors :+ orchestratorReceiveCommand).reduce(_ orElse _)
    context become newBehavior
  }

  def orchestratorReceiveCommand: Receive = LoggingReceive {
    case StartReadyTasks ⇒
      if (!earlyTerminated) {
        tasks.filter(_.canStart).foreach(_.start())

        if (tasks.forall(_.hasFinished)) {
          onFinish()
        }
      }
    case Status ⇒
      sender() ! StatusResponse(tasks.map(_.toTaskStatus))
    case SaveSnapshot ⇒
      saveSnapshot(_state)
  }
  def receiveCommand: Receive = orchestratorReceiveCommand

  def orchestratorReceiveRecover: Receive = LoggingReceive {
    case SnapshotOffer(metadata, offeredSnapshot: State) ⇒
      state = offeredSnapshot.asInstanceOf[State]
    case MessageSent(taskIndex) ⇒
      tasks(taskIndex).start()
    case MessageReceived(taskIndex, message, correlationId, deliveryId) ⇒
      val task = tasks(taskIndex)
      state = state.updatedIdsPerDestination(task.destination, correlationId -> deliveryId)
      task.behavior.apply(message)
    case RecoveryCompleted ⇒
      onRecoveryComplete()
  }
  def receiveRecover: Receive = orchestratorReceiveRecover
}