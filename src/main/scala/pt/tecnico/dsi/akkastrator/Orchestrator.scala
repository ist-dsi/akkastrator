package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorLogging, ActorPath}
import akka.event.LoggingReceive
import akka.persistence._
import pt.tecnico.dsi.akkastrator.Task.Waiting

case object StartReadyTasks
case object SaveSnapshot

sealed trait Event
case class MessageSent(taskIndex: Int) extends Event
case class MessageReceived(taskIndex: Int, response: Any, correlationId: CorrelationId, deliveryId: DeliveryId) extends Event

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
sealed abstract class AbstractOrchestrator(settings: Settings) extends PersistentActor with AtLeastOnceDelivery with ActorLogging with IdImplicits {
  type S <: State //The type of the state this orchestrator maintains
  type T <: AbstractTask[T] //The type of tasks this Orchestrator creates

  //This gets the Orchestrator started
  startTasks()

  private[akkastrator] final var earlyTerminated = false
  
  private[this] final var _tasks: IndexedSeq[T] = Vector.empty
  final def tasks: IndexedSeq[T] = _tasks
  private[akkastrator] final def addTask(task: T): Int = {
    val index = _tasks.length
    _tasks :+= task
    index
  }
  
  private[this] final var _state: S = _
  final def state: S = _state
  final def state_=(state: S): Unit = _state = state

  abstract class TaskProxy(val description: String, val dependencies: Set[TaskProxy] = Set.empty)
    extends AbstractTask[TaskProxy](this, description, dependencies) {
    final type ID = T#ID
    // The task is being added to the orchestrator in this line.
    // Because constructing a Task of type T also adds it to the orchestrator.
    val tTask: T = toT(this)
    def matchId(id: Long): Boolean = tTask.matchId(id) //This is where the magic happens

    // These methods will never be used, but we have to define them
    final val index: Int = tTask.index
    // This method cannot be implemented in a way that returns a value of type ID so its left unimplemented
    protected[akkastrator] final def deliveryId2ID(deliveryId: DeliveryId): ID = ???
    // This is a dummy implementation.
    protected[akkastrator] final def ID2Ids(id: ID): (CorrelationId, DeliveryId) = (0L, 0L)
  }
  def toT(proxy: TaskProxy): T

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
  def onEarlyTermination(instigator: T, message: Any, tasks: Map[Task.Status, Seq[T]]): Unit = ()

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

  final def orchestratorCommand: Actor.Receive = LoggingReceive {
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
  def extraCommands: Actor.Receive = PartialFunction.empty[Any, Unit]

  final def receiveCommand: Actor.Receive = orchestratorCommand orElse extraCommands

  final def receiveRecover: Actor.Receive = LoggingReceive {
    case SnapshotOffer(metadata, offeredSnapshot: State) ⇒
      state = offeredSnapshot.asInstanceOf[S]
    case MessageSent(taskIndex) ⇒
      tasks(taskIndex).start()
    case MessageReceived(taskIndex, message, correlationId, deliveryId) ⇒
      val task = tasks(taskIndex)
      recoverStateOnMessageReceived(task, correlationId, deliveryId)
      task.behavior(message)
    case RecoveryCompleted ⇒
      log.info(
        s"""Tasks after recovery completed:
          |\t${tasks.map(t ⇒ t.withLoggingPrefix(t.status.toString)).mkString("\n\t")}""".stripMargin)
  }
  
  protected[akkastrator] def recoverStateOnMessageReceived(task: T, correlationId: CorrelationId, deliveryId: DeliveryId): Unit
}

abstract class Orchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) {
  final type S = State //This orchestrator accepts any kind of State
  final type T = Task
  state = EmptyState

  abstract class Task(description: String, dependencies: Set[Task] = Set.empty) extends AbstractTask[Task](this, description, dependencies) {
    final type ID = DeliveryId
    final val index = addTask(this)
  
    protected[akkastrator] final def deliveryId2ID(deliveryId: DeliveryId): DeliveryId = deliveryId
    //In a SimpleOrchestrator we always use 0L for the correlationId since it is not used.
    protected[akkastrator] final def ID2Ids(id: DeliveryId): (CorrelationId, DeliveryId) = (0L, id)

    /** @return whether `id` is the same as the expectedDeliveryId. */
    final def matchDeliveryId(id: DeliveryId): Boolean = expectedDeliveryId.contains(id)
    /** This method simply calls `matchDeliveryId`. */
    final def matchId(id: Long): Boolean = matchDeliveryId(id)
  }

  final def toT(proxy: TaskProxy): T = new Task(proxy.description, proxy.dependencies.map(_.tTask)) {
    val destination: ActorPath = proxy.destination
    def createMessage(id: DeliveryId): Any = proxy.createMessage(id)
    def behavior: Actor.Receive = proxy.behavior
  }
  
  protected[akkastrator] final def recoverStateOnMessageReceived(task: Task, correlationId: CorrelationId,
                                                           deliveryId: DeliveryId): Unit = {
    //This orchestrator does not need to update the state
  }
}

abstract class DistinctIdsOrchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) {
  final type S = State with DistinctIds //This orchestrator requires that the state includes DistinctIds
  final type T = Task
  state = new MinimalState()

  abstract class Task(description: String, dependencies: Set[Task] = Set.empty) extends AbstractTask[Task](this, description, dependencies) {
    final type ID = CorrelationId
    final val index = addTask(this)
  
    protected[akkastrator] final def deliveryId2ID(deliveryId: DeliveryId): CorrelationId = {
      val correlationId: CorrelationId = state
        .getIdsFor(destination)
        .keySet.lastOption
        .map[CorrelationId](_.self + 1L)
        .getOrElse(0L)
  
      state = state.updatedIdsPerDestination(destination, correlationId → deliveryId)
      correlationId
    }
    protected[akkastrator] final def ID2Ids(id: CorrelationId): (CorrelationId, DeliveryId) = (id, getDeliveryId(id))

    private def getDeliveryId(correlationId: CorrelationId): DeliveryId = {
      state.idsPerDestination.get(destination).flatMap(_.get(correlationId)) match {
        case Some(deliveryId) ⇒ deliveryId
        case None ⇒ throw new IllegalArgumentException(
          s"""Could not obtain the delivery id for:
              |\tDestination: $destination
              |\tCorrelationId: $correlationId""".stripMargin)
      }
    }

    /**
      * @param id the correlationId obtained from the received message.
      * @return true if
      *         · This task status is Waiting
      *         · The actor path of the sender is the same as `destination`.
      *         · The deliveryId resolved from the correlationId is the same as the expectedDeliveryId.
      *        false otherwise.
      */
    final def matchSenderAndId(id: CorrelationId): Boolean = {
      lazy val senderPath = sender().path
      lazy val deliveryId = getDeliveryId(id)

      val matches = status == Waiting &&
        (if (recoveryRunning) true else senderPath == destination) &&
        expectedDeliveryId.contains(deliveryId)

      log.debug(withLoggingPrefix {
        val length = senderPath.toString.length max destination.toString.length
        String.format(
          s"""MatchSenderAndId:
              |CorrelationId: $id resolved to DeliveryId: $deliveryId
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
    /** This method simply calls `matchSenderAndId`. */
    final def matchId(id: Long): Boolean = matchSenderAndId(id)
  }

  final def toT(proxy: TaskProxy): T = new Task(proxy.description, proxy.dependencies.map(_.tTask)) {
    val destination: ActorPath = proxy.destination
    def createMessage(id: CorrelationId): Any = proxy.createMessage(id)
    def behavior: Actor.Receive = proxy.behavior
  }
  
  protected[akkastrator] final def recoverStateOnMessageReceived(task: Task, correlationId: CorrelationId,
                                                           deliveryId: DeliveryId): Unit = {
    state = state.updatedIdsPerDestination(task.destination, correlationId -> deliveryId)
  }
}