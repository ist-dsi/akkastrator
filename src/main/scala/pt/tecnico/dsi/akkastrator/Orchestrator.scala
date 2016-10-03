package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorLogging, ActorPath}
import akka.persistence._

object Orchestrator {
  case class StartOrchestrator(id: Long)
   
  case class TasksFinished[R](result: R, id: Long)
  case class TasksAborted(instigatorReport: TaskReport, cause: AbortCause, id: Long)
  
  case object SaveSnapshot
  
  case object Status
  case class StatusResponse(tasks: IndexedSeq[TaskReport])
  
  case object ShutdownOrchestrator
}

/**
  * An Orchestrator executes a set of, possibly dependent, `Tasks`.
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
  *   trait DatabaseTasks { self: AbstractOrchestrator[_] =>
  *     def createQueryTask(): Task = new Task("") {
  *       val destination: ActorPath = ???
  *       def createMessage(correlationId: CorrelationId): Any = ???
  *
  *       def behavior: Receive = ???
  *     }
  *   }
  * }}}
  */
sealed abstract class AbstractOrchestrator[R](val settings: Settings) extends PersistentActor with AtLeastOnceDelivery
  with ActorLogging with IdImplicits {
  import Orchestrator._
  
  /** The type of the state this orchestrator maintains. */
  type S <: State
  /** The type of Id this orchestrator handles. */
  type ID <: Id
  
  private[akkastrator] case object StartReadyTasks
  
  /** This exists to make the creation of tasks easier. */
  final implicit val orchestrator = this
  
  private[this] final var _tasks: IndexedSeq[Task[_]] = Vector.empty
  private[akkastrator] final def tasks: IndexedSeq[Task[_]] = _tasks
  private[akkastrator] final def upsertTask(task: Task[_]): Int = tasks.indexOf(task) match {
    case -1 =>
      _tasks :+= task
      tasks.length - 1
    case index => index
  }
  
  private[this] final var _state: S = _
  /** Gets the state this orchestrator currently has. */
  final def state: S = _state
  /** Sets the new state for this orchestrator. */
  final def state_=(state: S): Unit = _state = state
  
  //The id obtained in the StartOrchestrator message which prompted the execution of this orchestrator tasks
  private[this] final var _startId: Long = _
  private[akkastrator] def startId: Long = _startId
  
  //Used to ensure inner orchestrators have different names and persistent ids
  private[this] final var _innerOrchestratorsLastId: Int = 0
  private[akkastrator] final def nextInnerOrchestratorId(): Int = {
    val id = _innerOrchestratorsLastId
    _innerOrchestratorsLastId += 1
    id
  }
  
  /**
    * Roughly every X messages a snapshot will be saved. Set to 0 to disable automatic saving of snapshots.
    * By default this method returns the value defined in the configuration.
    *
    * This is just a rough value because the orchestrator will not save it in the snapshots.
    * In fact it will not save it at all. Instead the value of `lastSequenceNr` will be used to estimate
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
  protected[akkastrator] def matchId(task: Task[_], id: Long): Boolean
  
  /**
    * User overridable callback. Its called every time a task finishes.
    *
    * You can use this to implement very refined termination strategies.
    *
    * If a task aborts this method will not be invoked.
    */
  def onTaskFinish(finishedTask: Task[_]): Unit = ()
  
  /**
    * User overridable callback. Its called once every task finishes.
    * By default logs that the Orchestrator has finished then stops it.
    *
    * You can use this to implement your termination strategy.
    *
    * If a orchestrator starts without tasks it will finish right away.
    */
  def onFinish(): Unit = {
    log.info(s"${self.path.name} Finished!")
    context stop self
  }
  
  /**
    * User overridable callback. Its called when a task instigates an abort.
    * By default logs that the Orchestrator has aborted then stops it.
    *
    * You can use this to implement your termination strategy.
    *
    * Note: if you invoke become/unbecome inside this method, the contract that states
    *       <cite>"Tasks that are waiting will remain untouched and the orchestrator will
    *       still be prepared to handle their responses"</cite> will no longer be guaranteed.
    *       If you wish to still have this guarantee you can do {{{
    *         context.become(computeCurrentBehavior() orElse yourBehavior)
    *       }}}
    *
    * @param instigator the task that instigated the abort.
    * @param message the message that caused the abort.
    * @param tasks Map with the tasks states at the moment of abort.
    */
  def onAbort(instigator: Task[_], message: Any, cause: AbortCause, tasks: Map[Task.State, Seq[Task[_]]]): Unit = {
    log.info(s"${self.path.name} Aborted due to $cause!")
    context.parent ! TasksAborted(instigator.toTaskReport, cause, startId)
    context stop self
  }
  
  /**
    * Every task will become Unstarted and the tasks will start again.
    *
    * Restart can only be invoked if all tasks have finished or a task caused an abort. If there is a task
    * that is still waiting invoking this method will throw an exception.
    */
  final def restart(): Unit = {
    //TODO: since require throws an exception it might cause a crash cycle
    require(tasks.exists(_.hasAborted) || tasks.forall(_.hasFinished),
      "An orchestrator can only restart if all tasks have finished or a task caused an abort.")
    tasks.foreach(_.state = Task.Unstarted)
    self ! StartReadyTasks
  }
  
  /** @return the behaviors of the tasks which are waiting plus `orchestratorCommand` and `extraCommands`. */
  final def computeCurrentBehavior(): Receive = {
    val waitingTaskBehaviors = tasks.filter(_.isWaiting).map(_.behavior)
    // OrchestratorCommand is first receive to guarantee that StartOrchestrator, ShutdownOrchestrator,
    // StartReadyTasks, Status or SaveSnapshot won't be taken first by one of the tasks behaviors or the extraCommands.
    // Similarly extraCommands is the last receive to ensure it doesn't take one of the messages of the waiting task behaviors.
    // The reduce will never fail because the orchestratorCommand always exists.
    (orchestratorCommand +: waitingTaskBehaviors :+ extraCommands).reduce(_ orElse _)
  }

  final def orchestratorCommand: Actor.Receive = /*LoggingReceive.withLabel("orchestratorCommand")*/ {
    case m @ StartOrchestrator(id) ⇒
      persist(m) { _ ⇒
        _startId = id
        self ! StartReadyTasks
      }
    case ShutdownOrchestrator ⇒
      context stop self
    case StartReadyTasks ⇒
      //TODO: if we could somehow implement this with context.become it would be more efficient
      //TODO: since the filter(_.canStart) and tasks.forall(_.hasFinished) will re-evaluate the same tasks over and over again
      /*
        * def orchestratorCommand(unstartedTasks: Seq[Task], terminatedEarly: Boolean = false): Actor.Receive = {
        *   case StartReadyTasks =>
        *     // The problem is that start, finish and abort will have to invoke orchestratorCommand(...)
        *     unstartedTasks.foreach(_.start())
        *     if (terminatedEarly == false && unstartedTasks.length == 0) {
        *       onFinish()
        *     }
        * }
        */
      
      tasks.filter(_.canStart).foreach(_.start())
      
      // This message is sent whenever a task finishes, so it is a very appropriate location to place
      // the computation of whether we should perform an automatic snapshot.
      // It is modulo (saveSnapshotEveryXMessages * 2) because we persist MessageSent and MessageReceived,
      // however we are only interested in MessageReceived. This will roughly correspond to every X MessageReceived.
      if (saveSnapshotRoughlyEveryXMessages > 0 && lastSequenceNr % (saveSnapshotRoughlyEveryXMessages * 2) == 0) {
        self ! SaveSnapshot
      }
      
      if (tasks.forall(_.hasFinished)) {
        onFinish()
      }
    case Status ⇒
      sender() ! StatusResponse(tasks.map(_.toTaskReport))
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
    case StartOrchestrator(id) ⇒
      _startId = id
      self ! StartReadyTasks
    case RecoveryCompleted ⇒
      if (tasks.nonEmpty) {
        val tasksString = tasks.map(t ⇒ t.withLoggingPrefix(t.state.toString)).mkString("\n\t")
        log.debug( s"""${self.path.name} - recovery completed:
                      |\t$tasksString
                      |\t#Unconfirmed: $numberOfUnconfirmed""".stripMargin)
      }
  }
}

abstract class Orchestrator[R](settings: Settings = new Settings()) extends AbstractOrchestrator[R](settings) {
  /** This orchestrator accepts any kind of State. */
  final type S = State
  /** This orchestrator uses DeliveryId directly because the same sequence number (of the akka-persistence)
    * is used for all of its destinations. */
  final type ID = DeliveryId
  state = EmptyState
  
  /**
    * In a simple orchestrator the same sequence number (of the akka-persistence) is used for all the
    * destinations of the orchestrator. Because of this, ID = DeliveryId, and matchId only checks the deliveryId
    * as that will be enough information to disambiguate which task should handle the response.
    */
  
  protected[akkastrator] final def deliveryId2ID(destination: ActorPath, deliveryId: DeliveryId): DeliveryId = deliveryId
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = id
  protected[akkastrator] def matchId(task: Task[_], id: Long): Boolean = {
    val deliveryId: DeliveryId = id
    val matches = task.expectedDeliveryId.contains(deliveryId)
    
    log.debug(task.withLoggingPrefix{
      String.format(
        s"""matchId:
            |          │ DeliveryId
            |──────────┼─────────────────
            |    VALUE │ %s
            | EXPECTED │ %s
            | Matches: %s""".stripMargin,
        Some(deliveryId), task.expectedDeliveryId,
        matches.toString.toUpperCase
      )
    })
    matches
  }
}

abstract class DistinctIdsOrchestrator[R](settings: Settings = new Settings()) extends AbstractOrchestrator[R](settings) {
  /** This orchestrator requires that the state includes DistinctIds. */
  final type S = State with DistinctIds
  /** This orchestrator uses CorrelationId for its ID. This is needed to ensure every
    * destination sees an independent sequence. */
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
    
    state = state.updatedIdsPerDestination(destination, correlationId -> deliveryId)
    log.debug(s"State for $destination is now:\n\t" + state.getIdsFor(destination).mkString("\n\t"))
    
    correlationId
  }
  protected[akkastrator] final def ID2DeliveryId(destination: ActorPath, id: Long): DeliveryId = {
    state.getDeliveryIdFor(destination, id)
  }
  protected[akkastrator] def matchId(task: Task[_], id: Long): Boolean = {
    lazy val senderPath = sender().path
    val correlationId: CorrelationId = id
    lazy val deliveryId = state.getDeliveryIdFor(task.destination, correlationId)
  
    val matches = (if (recoveryRunning) true else senderPath == task.destination) &&
      task.state == Task.Waiting && task.expectedDeliveryId.contains(deliveryId)
  
    log.debug(task.withLoggingPrefix{
      val senderPathString = senderPath.toStringWithoutAddress
      val destinationString = task.destination.toStringWithoutAddress
      val length = senderPathString.length max destinationString.length
      String.format(
        s"""MatchId:
            |($destinationString, $correlationId) resolved to $deliveryId
            |          │ %${length}s │ DeliveryId
            |──────────┼─%${length}s─┼──────────────────────────────
            |    VALUE │ %${length}s │ %s
            | EXPECTED │ %${length}s │ %s
            | Matches: %s""".stripMargin,
        "SenderPath",
        "─" * length,
        senderPathString, Some(deliveryId),
        destinationString, task.expectedDeliveryId,
        matches.toString.toUpperCase + (if (recoveryRunning) " because recovery is running." else "")
      )
    })
    matches
  }
}