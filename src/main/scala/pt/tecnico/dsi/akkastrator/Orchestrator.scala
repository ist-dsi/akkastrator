package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorLogging, ActorPath}
import akka.persistence._

case object StartReadyTasks
case object SaveSnapshot
case object Status
case class StatusResponse(tasks: IndexedSeq[TaskView])

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
  with ActorLogging with IdImplicits {
  //The type of the state this orchestrator maintains
  type S <: State
  //The type of Id this orchestrator handles
  type ID <: Id
  
  //This exists to make the creation of tasks easier
  final implicit val orchestrator = this
  
  private[this] final var _tasks: IndexedSeq[Task] = Vector.empty
  private[this] final var _state: S = _
  
  private[akkastrator] final def tasks: IndexedSeq[Task] = _tasks
  private[akkastrator] final def addTask(task: Task): Int = {
    val index = tasks.length
    _tasks :+= task
    index
  }
  
  final def state: S = _state
  final def state_=(state: S): Unit = _state = state
  
  //This gets the Orchestrator started
  startTasks()
  
  /**
    * Roughly Every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * By default this method returns the value defined in the configuration.
    *
    * This is just a rough value because the orchestrator will not save it in the snapshots.
    * In fact it will not save it at all. Instead the value of lastSequenceNr will be used to estimate
    * how many messages have been processed.
    *
    * You can trigger a save snapshot manually by sending a `SaveSnapshot` message to this orchestrator.
    */
  def saveSnapshotRoughlyEveryXMessages: Int = settings.saveSnapshotRoughlyEveryXMessages
  
  /**
    * Converts the deliveryId obtained from the deliver method of akka-persistence to ID.
    * Also updates this orchestrator state if necessary.
    */
  protected[akkastrator] def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): ID
  /** Converts ID to the deliveryId needed for the confirmDelivery method of akka-persistence. */
  protected[akkastrator] def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId
  protected[akkastrator] def matchId(task: Task, id: Long): Boolean
  
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
    *
    * An Orchestrator without tasks never finishes.
    */
  def onFinish(): Unit = {
    log.info(s"Orchestrator Finished!")
    context stop self
  }
  
  /**
    * User overridable callback. Its called when a task requests an early termination.
    * By default logs that the Orchestrator has terminated early then stops it.
    *
    * @param instigator the task that initiated the early termination.
    * @param message the message that caused the early termination.
    * @param tasks Map with the tasks status at the moment of early termination.
    * @note if you invoke become/unbecome inside this method, the contract that states
    *       <cite>"Tasks that are waiting will remain untouched and the orchestrator will
    *       still be prepared to handle their responses"</cite> will no longer be guaranteed.
    */
  def onEarlyTermination(instigator: Task, message: Any, tasks: Map[TaskState, Seq[Task]]): Unit = {
    log.info(s"Orchestrator Terminated Early!")
    context stop self
  }

  /** @return the behaviors of the tasks which are waiting plus `orchestratorCommand`. */
  private[akkastrator] final def updateCurrentBehavior(): Unit = {
    val waitingTaskBehaviors = tasks.filter(_.isWaiting).map(_.behavior)
    // By folding left we ensure orchestratorCommand is always the first receive.
    // This is important to guarantee that StartReadyTasks, Status or SaveSnapshot won't be taken first
    // by one of the tasks behaviors. The extraCommands will come last, after the waiting task behaviors.
    val newBehavior = (waitingTaskBehaviors :+ extraCommands).foldLeft(orchestratorCommand)(_ orElse _)
    context become newBehavior

    // This method is invoked whenever a task finishes, so it is a very appropriate location to place
    // the computation of whether we should perform an automatic snapshot.
    // It is modulo (saveSnapshotEveryXMessages * 2) because we persist MessageSent and MessageReceived,
    // however we are only interested in MessageReceived. This will roughly correspond to every X MessageReceived.
    if (saveSnapshotRoughlyEveryXMessages > 0 && lastSequenceNr % (saveSnapshotRoughlyEveryXMessages * 2) == 0) {
      self ! SaveSnapshot
    }
  }

  final def orchestratorCommand: Actor.Receive = /*LoggingReceive.withLabel("orchestratorCommand")*/ {
    //TODO: if we could somehow implement this with context.become it would be more efficient
    //TODO: since the filter(_.canStart) and tasks.forall(_.hasFinished) will re-evaluate the same tasks over and over again
    /**
      * def orchestratorCommand(unstartedTasks: Seq[Task], terminatedEarly: Boolean = false): Actor.Receive = {
      *   case StartReadyTasks =>
      *     // The problem is that start, finish and terminatedEarly will have to invoke orchestratorCommand(...)
      *     unstartedTasks.foreach(_.start())
      *     if (terminatedEarly == false && unstartedTasks.length == 0) {
      *       onFinish()
      *     }
      * }
      */
    case StartReadyTasks ⇒
      tasks.filter(_.canStart).foreach(_.start())
      if (tasks.nonEmpty && tasks.forall(_.hasFinished)) {
        onFinish()
      }
    case Status ⇒
      sender() ! StatusResponse(tasks.map(_.toTaskView))
    case SaveSnapshot ⇒
      saveSnapshot(_state)
  }

  /**
    * Override this method to add extra commands that are always handled by this orchestrator (except when recovering).
    */
  def extraCommands: Actor.Receive = PartialFunction.empty[Any, Unit]

  final def receiveCommand: Actor.Receive = orchestratorCommand orElse extraCommands

  def receiveRecover: Actor.Receive = /*LoggingReceive.withLabel("receiveRecover")*/ {
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
      val tasksString = tasks.map(t ⇒ t.withLoggingPrefix(t.state.toString)).mkString("\n\t")
      log.info(s"Tasks after recovery completed:\n\t$tasksString\n\t#Unconfirmed: $numberOfUnconfirmed")
  }
}

abstract class Orchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) {
  final type S = State //This orchestrator accepts any kind of State
  final type ID = DeliveryId
  state = EmptyState
  
  /**
    * In a simple orchestrator the same sequence (of the akka-persistence) is used for all the destinations of the
    * orchestrator. Because of this, ID = DeliveryId, and matchId only checks the deliveryId
    * as that will be enough information to disambiguate which task should handle the response.
    */
  
  protected[akkastrator] final def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): DeliveryId = deliveryId
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = id
  protected[akkastrator] def matchId(task: Task, id: Long): Boolean = {
    val matches = task.state match {
      case Waiting(expectedDeliveryId) if expectedDeliveryId.self == id ⇒ true
      case _ ⇒ false
    }
    
    log.debug(task.withLoggingPrefix {
      val length = task.state.toString.length
      String.format(
        s"""matchDeliveryId:
            | FIELD │ %${length}s │ EXPECTED VALUE
            |───────┼─%${length}s─┼────────────────
            | State │ %${length}s │ %-${length}s
            | Matches: $matches""".stripMargin,
        "VALUE",
        "─" * length,
        task.state, Waiting(id)
      )
    })
    matches
  }
}

abstract class DistinctIdsOrchestrator(settings: Settings = new Settings()) extends AbstractOrchestrator(settings) {
  final type S = State with DistinctIds //This orchestrator requires that the state includes DistinctIds
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
    val correlationId = state.getNextCorrelationIdFor(destination)
    
    state = state.updatedIdsPerDestination(destination, correlationId → deliveryId)
    log.debug("New State:\n\t" + state.idsPerDestination.mkString("\n\t"))
    
    correlationId
  }
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = {
    state.getDeliveryIdFor(destination, id)
  }
  protected[akkastrator] def matchId(task: Task, id: Long): Boolean = {
    lazy val senderPath = sender().path
    lazy val deliveryId = state.getDeliveryIdFor(task.destination, id)
  
    val matches = (if (recoveryRunning) true else senderPath == task.destination) && (task.state match {
      case Waiting(expectedDeliveryId) if expectedDeliveryId == deliveryId ⇒ true
      case _ ⇒ false
    })
  
    log.debug(task.withLoggingPrefix {
      val length = senderPath.toString.length max task.destination.toString.length
      String.format(
        s"""MatchSenderAndId:
            |CorrelationId($id) resolved to DeliveryId($deliveryId)
            |      FIELD │ %${length}s │ EXPECTED VALUE
            |────────────┼─%${length}s─┼────────────────
            | SenderPath │ %${length}s │ %-${length}s
            |      State │ %${length}s │ %-${length}s
            | Matches: $matches %s""".stripMargin,
        "VALUE",
        "─" * length,
        senderPath, task.destination,
        task.state, Waiting(deliveryId),
        if (recoveryRunning) "because recovery is running." else ""
      )
    })
    matches
  }
}