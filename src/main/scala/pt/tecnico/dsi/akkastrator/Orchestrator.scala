package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorLogging, ActorPath}
import akka.persistence._
import pt.tecnico.dsi.akkastrator.Task.Waiting

case object StartReadyTasks
case object SaveSnapshot

sealed trait Event
case class MessageSent(taskIndex: Int) extends Event
case class MessageReceived(taskIndex: Int, response: Any) extends Event

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
sealed abstract class AbstractOrchestrator(settings: Settings) extends PersistentActor with AtLeastOnceDelivery
  with ActorLogging with IdImplicits with OrchestratorContext {
  
  type S <: State //The type of the state this orchestrator maintains
  type T <: AbstractTask[T] //The type of tasks this orchestrator creates
  type ID <: Id //The type of Id this orchestrator handles
  
  //This gets the Orchestrator started
  startTasks()

  private[akkastrator] final var earlyTerminated = false
  
  private[this] final var _tasks: IndexedSeq[T] = Vector.empty
  final def tasks: IndexedSeq[T] = _tasks
  private[akkastrator] final def addTask(task: T): Unit = _tasks :+= task
  
  private[this] final var _state: S = _
  final def state: S = _state
  final def state_=(state: S): Unit = _state = state
    
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
  
  
  abstract class TaskProxy(description: String, dependencies: Set[TaskProxy] = Set.empty)
    extends AbstractTask[TaskProxy](description, dependencies) {
    /**
      * This class exists to fulfill the following purposes:
      *  1) Allow the refactoring of task creation in a manner that is agnostic to the type of orchestrator.
      *     To do so the created task must behave in the same way as the tasks created in the concrete orchestrator.
      *     More specifically a TaskProxy that is mixed-in in a "Simple" Orchestrator must match just the deliveryId,
      *     However the same TaskProxy mixed-in in a DistinctIdsOrchestrator must match the sender and the correlationId.
      *  2) It must be possible to create a TaskProxy that depends on other TaskProxies.
      *  3) It must be possible to create a Task that depends on a TaskProxy.
      *  4) It must be possible to create a TaskProxy that depends on a Task.
      */
  
    /**
      * The default implementation just calls toTask, which creates an instance of T and adds it to the concrete
      * orchestrator.
      */
    @inline private[akkastrator] def createTask(): T = toTask(this)
    @inline val realTask: T = createTask()
    
    @inline def matchId(id: Long): Boolean = realTask.matchId(id)
  }
  /** Converts the TaskProxy to the type of tasks the concrete subclass of this orchestrator creates. */
  def toTask(proxy: TaskProxy): T
  
  //This allows using a task as a dependency for a TaskProxy
  implicit final def task2TaskProxy(task: T): TaskProxy = new TaskProxy(task.description, task.dependencies.map(task2TaskProxy)) {
    val destination: ActorPath = task.destination
    def createMessage(id: ID): Any = task.createMessage(id)
    def behavior: Receive = task.behavior
    
    //By overriding this method we ensure this task is not re-added to the orchestrator.
    @inline override private[akkastrator] final def createTask(): T = task
  }
  //This allows using a TaskProxy as a dependency for a Task
  @inline implicit final def taskProxy2Task(taskProxy: TaskProxy): T = taskProxy.realTask
  
  /**
    * Converts the deliveryId obtained from the deliver method of akka-persistence to ID.
    * Also updates this orchestrator state if necessary.
    */
  protected[akkastrator] def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): ID
  /** Converts ID to the deliveryId needed for the confirmDelivery method of akka-persistence. */
  protected[akkastrator] def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId
  
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
  def onEarlyTermination(instigator: T, message: Any, tasks: Map[Task.State, Seq[T]]): Unit = ()

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

  final def orchestratorCommand: Actor.Receive = /*LoggingReceive.withLabel("orchestratorCommand")*/ {
    case StartReadyTasks ⇒
      if (!earlyTerminated) {
        tasks.filter(_.canStart).foreach(_.start())
        if (tasks.forall(_.hasFinished)) {
          onFinish()
        }
      }
    case Status ⇒
      sender() ! StatusResponse(tasks.map(_.toTaskDescription))
    case SaveSnapshot ⇒
      saveSnapshot(_state)
  }

  /**
    * Override this method to add extra commands that are always handled by this orchestrator (except when recovering).
    */
  def extraCommands: Actor.Receive = PartialFunction.empty[Any, Unit]

  final def receiveCommand: Actor.Receive = orchestratorCommand orElse extraCommands

  final def receiveRecover: Actor.Receive = /*LoggingReceive.withLabel("receiveRecover")*/ {
    case SnapshotOffer(metadata, offeredSnapshot: State) ⇒
      state = offeredSnapshot.asInstanceOf[S]
    case MessageSent(taskIndex) ⇒
      val task = tasks(taskIndex)
      log.info(task.withLoggingPrefix(s"Recovering MessageSent."))
      task.start()
    case MessageReceived(taskIndex, message) ⇒
      val task = tasks(taskIndex)
      log.info(task.withLoggingPrefix(s"Recovering MessageReceived."))
      task.behavior(message)
    case RecoveryCompleted ⇒
      val tasksString = tasks.map { t ⇒
        t.withLoggingPrefix(f"${t.state}%-9s (Expected DeliveryId: ${t.expectedDeliveryId})")
      }.mkString("\n\t")
      log.info(s"Tasks after recovery completed:\n\t$tasksString\n\t#Unconfirmed: $numberOfUnconfirmed")
  }
}

abstract class Orchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) {
  final type S = State //This orchestrator accepts any kind of State
  final type T = Task
  final type ID = DeliveryId
  state = EmptyState
  
  /**
    * In a simple orchestrator the same sequence (of the akka-persistence) is used for all the destinations of the
    * orchestrator. Because of this, ID = DeliveryId, and matchId only checks the deliveryId
    * as that will be enough information to disambiguate which task should handle the response.
    */
  
  protected[akkastrator] final def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): DeliveryId = deliveryId
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = id
  
  abstract class Task(description: String, dependencies: Set[Task] = Set.empty) extends AbstractTask[Task](description, dependencies) {
    addTask(this)
    
    /** @return whether `expectedDeliveryId` contains `id`. */
    final def matchDeliveryId(id: DeliveryId): Boolean = {
      val matches = expectedDeliveryId.contains(id)
  
      log.debug(withLoggingPrefix {
        val length = expectedDeliveryId.toString.length max 6
        String.format(
          s"""matchDeliveryId:
              |      FIELD │ %${length}s │ EXPECTED VALUE
              |────────────┼─%${length}s─┼────────────────
              | DeliveryId │ %${length}s │ %-${length}s
              | Matches: $matches""".stripMargin,
          "VALUE",
          "─" * length,
          id.toString, expectedDeliveryId
        )
      })
      matches
    }
    /** This method simply calls `matchDeliveryId`. */
    final def matchId(id: Long): Boolean = matchDeliveryId(id)
  }
  
  final def toTask(proxy: TaskProxy): T = new Task(proxy.description, proxy.dependencies.map(_.realTask)) {
    val destination: ActorPath = proxy.destination
    def createMessage(id: DeliveryId): Any = proxy.createMessage(id)
    def behavior: Actor.Receive = proxy.behavior
  }
}

abstract class DistinctIdsOrchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) { orchestrator ⇒
  final type S = State with DistinctIds //This orchestrator requires that the state includes DistinctIds
  final type T = Task
  final type ID = CorrelationId
  state = new MinimalState()
  
  /**
    * In a DistinctIdsOrchestrator an independent sequence is used for each destination of the orchestrator.
    * To be able to achieve this, an extra state is needed: the DistinctIds.
    * In this orchestrator the delivery id is not sufficient to disambiguate which task should handle the message,
    * so the ID = CorrelationId and the matchId also needs to check the sender.
    * There is also the added necessity of being able to compute the correlation id for a given (sender, deliveryId)
    * as well as translating a correlation id back to a delivery id.
    */
  
  protected[akkastrator] final def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): CorrelationId = {
    val correlationId = orchestrator.state.getNextCorrelationIdFor(destination)
    
    orchestrator.state = orchestrator.state.updatedIdsPerDestination(destination, correlationId → deliveryId)
    log.debug("New State:\n\t" + orchestrator.state.idsPerDestination.mkString("\n\t"))
    
    correlationId
  }
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = {
    state.getDeliveryIdFor(destination, id)
  }
  
  abstract class Task(description: String, dependencies: Set[Task] = Set.empty) extends AbstractTask[Task](description, dependencies) {
    addTask(this)
  
    /**
      * @param id the correlationId obtained from the received message.
      * @return true if
      *         1. This task status is Waiting
      *         2. The actor path of the sender is the same as `destination`.
      *         3. The `expectedDeliveryId` contains the deliveryId resolved from the correlationId.
      *        false otherwise.
      */
    final def matchSenderAndId(id: CorrelationId): Boolean = {
      lazy val senderPath = sender().path
      lazy val deliveryId = orchestrator.state.getDeliveryIdFor(destination, id)

      val matches = state == Waiting &&
        (if (recoveryRunning) true else senderPath == destination) &&
        expectedDeliveryId.contains(deliveryId)

      log.debug(withLoggingPrefix {
        val length = senderPath.toString.length max destination.toString.length
        String.format(
          s"""MatchSenderAndId:
              |CorrelationId($id) resolved to DeliveryId($deliveryId)
              |      FIELD │ %${length}s │ EXPECTED VALUE
              |────────────┼─%${length}s─┼────────────────
              |     Status │ %${length}s │ Waiting
              | SenderPath │ %${length}s │ %-${length}s
              | DeliveryId │ %${length}s │ %-${length}s
              | Matches: $matches %s""".stripMargin,
          "VALUE",
          "─" * length,
          state,
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
  
  final def toTask(proxy: TaskProxy): T = new Task(proxy.description, proxy.dependencies.map(_.realTask)) {
    val destination: ActorPath = proxy.destination
    def createMessage(id: CorrelationId): Any = proxy.createMessage(id)
    def behavior: Actor.Receive = proxy.behavior
  }
}