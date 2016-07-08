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
    //This must be a val to ensure the returned value is always the same.
    val idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]

    /**
      * @return a new copy of State with the new IdsPerDestination.
      */
    def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): State

    /**
      * Get the SorteMap relation between CorrelationId and DeliveryId for the given `destination`.
      */
    def getIdsFor(destination: ActorPath): SortedMap[CorrelationId, DeliveryId] = {
      idsPerDestination.getOrElse(destination, SortedMap.empty[CorrelationId, DeliveryId])
    }

    /**
      * @return a new copy of State with the IdsPerDestination updated for `destination` using the `newIdRelation`.
      */
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
  *  - Delivering messages with at-least-once delivery guarantee. The Orchestrator ensures each destination
  *    will see an independent strictly monotonically increasing sequence number without gaps.
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

  //This gets the Orchestrator started
  startTasks()

  private[akkastrator] var earlyTerminated = false

  private var _tasks: IndexedSeq[Task] = Vector.empty
  private[akkastrator] def addTask(task: Task): Int = {
    val index = _tasks.length
    _tasks :+= task
    index
  }
  def tasks: IndexedSeq[Task] = _tasks

  private[this] var _state: State = new MinimalState()
  def state[S <: State]: S = _state.asInstanceOf[S]
  def state_=[S <: State](state: S): Unit = _state = state

  /**
    * Every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * By default this method returns the value defined in the configuration.
    *
    * Roughly every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * This is just a rough value because the orchestrator will not save it in the snapshots.
    * In fact it will not save it at all. Instead the value of lastSequenceNr will be used to estimate
    * how many messages have been processed.
    *
    * You can trigger a save snapshot manually by sending a `SaveSnapshot` message to this orchestrator.
    */
  def saveSnapshotRoughlyEveryXMessages: Int = settings.saveSnapshotRoughlyEveryXMessages

  /**
    * User overridable callback. Its called to start the Tasks.
    * By default logs that the orchestrator started and sends it the `StartReadyTasks` message.
    *
    * You can override this to prevent the Orchestrator from starting right away.
    * However that strategy will only be effective the first time the orchestrator starts, that is,
    * if this orchestrator restarts with one task already finished, then that task will send the
    * `StartReadyTasks` so that tasks that depend on it can start.
    */
  def startTasks(): Unit = {
    log.info(s"Started ${this.getClass.getSimpleName} with PersistenceId: $persistenceId")
    self ! StartReadyTasks
  }

  /**
    * User overridable callback. Its called once every task finishes.
    * By default logs that the Orchestrator has finished then stops it.
    *
    * You can use this to implement your termination strategy.
    */
  def onFinish(): Unit = {
    log.info(s"Orchestrator Finished!")
    context stop self
  }

  //TODO: can we guarantee that waiting tasks will still process their responses even if the user performs become/unbecome?
  /**
    * User overridable callback. Its called when a task requests an early termination. Empty default implementation.
    *
    * @param instigator the task that initiated the early termination.
    * @param message the message that caused the early termination.
    * @param tasks Map with the tasks status at the moment of early termination.
    * @note if you invoke become/unbecome inside this method, the contract that states
    *       <cite>"Tasks that are waiting will remain untouched and the orchestrator will
    *       still be prepared to handle their responses"</cite> will no longer be guaranteed.
    */
  def onEarlyTermination(instigator: Task, message: Any, tasks: Map[Task.Status, Seq[Task]]): Unit = ()

  /** @return the behaviors of the tasks which are waiting plus `orchestratorCommand`. */
  private[akkastrator] final def updateCurrentBehavior(): Unit = {
    val waitingTaskBehaviors = tasks.filter(_.isWaiting).map(_.behavior)
    // By folding left we ensure orchestratorCommand is always the first receive.
    // This is important to guarantee that StartReadyTasks, Status or SaveSnapshot won't be taken first
    // by one of the tasks behaviors.
    val newBehavior = waitingTaskBehaviors.foldLeft(orchestratorCommand)(_ orElse _)
    // Extra commands will always come last
    context become (newBehavior orElse extraCommands)

    // This method is invoked whenever a task finishes, so it is a very appropriate location to place
    // the computation of whether we should perform an automatic snapshot.
    // It is modulo (saveSnapshotEveryXMessages * 2) because we persist MessageSent and MessageReceived,
    // however we are only interested in MessageReceived. This will roughly correspond to every X MessageReceived.
    if (saveSnapshotRoughlyEveryXMessages > 0 && lastSequenceNr % (saveSnapshotRoughlyEveryXMessages * 2) == 0) {
      self ! SaveSnapshot
    }
  }

  final def orchestratorCommand: Receive = LoggingReceive {
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

  /**
    * Override this method to add extra commands that are always handled by this orchestrator (except when recovering).
    */
  def extraCommands: Receive = PartialFunction.empty[Any, Unit]

  final def receiveCommand: Receive = orchestratorCommand orElse extraCommands

  final def receiveRecover: Receive = LoggingReceive {
    case SnapshotOffer(metadata, offeredSnapshot: State) ⇒
      state = offeredSnapshot.asInstanceOf[State]
    case MessageSent(taskIndex) ⇒
      tasks(taskIndex).start()
    case MessageReceived(taskIndex, message, correlationId, deliveryId) ⇒
      val task = tasks(taskIndex)
      state = state.updatedIdsPerDestination(task.destination, correlationId -> deliveryId)
      task.behavior(message)
    case RecoveryCompleted ⇒
      log.info(
        s"""Tasks after recovery completed:
          |\t${tasks.map(t ⇒ t.withLoggingPrefix(t.status.toString)).mkString("\n\t")}""".stripMargin)
  }
}