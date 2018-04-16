package pt.tecnico.dsi.akkastrator

import scala.collection.{immutable, mutable}
import scala.collection.immutable.HashMap

import akka.actor.{Actor, ActorLogging, ActorPath, PossiblyHarmful}
import akka.persistence._
import shapeless.HNil

object Orchestrator {
  case class StartOrchestrator(id: Long)
  
  trait Success[R] extends Serializable {
    def id: Long
    def result: R
  }
  case class Finished[R](result: R, id: Long) extends Success[R]
  
  trait Failure extends Serializable {
    def id: Long
    def cause: Throwable
  }
  case class Aborted(cause: Throwable, id: Long) extends Failure
  case class TaskAborted[R](instigatorReport: Task.Report[R], cause: Throwable, id: Long) extends Failure
  
  case object Status
  case class StatusResponse(tasks: Seq[Task.Report[_]])
  
  case object SaveSnapshot
  case object ShutdownOrchestrator extends PossiblyHarmful
  
  protected[akkastrator] case class StartTask(index: Int)
}

/**
  * An Orchestrator executes a set of, possibly dependent, `Tasks`.
  * A task corresponds to sending a message to an actor, handling its response and possibly
  * mutate the internal state of the Orchestrator.
  *
  * The Orchestrator together with the Task is able to:
  *
  *  - Handling the persistence of the internal state maintained by both the Orchestrator and the Tasks.
  *  - Delivering messages with at-least-once delivery guarantee. The `DistinctIdsOrchestrator` ensures each destination
  *    will see an independent strictly monotonically increasing sequence number without gaps.
  *  - Handling Status messages, that is, if some actor is interested in querying the Orchestrator for its current
  *    status, the Orchestrator will respond with the status of each task.
  *  - When all the dependencies of a task finish that task will be started and the Orchestrator will
  *    be prepared to handle the messages that the task destination sends.
  *  - If the Orchestrator crashes, the state it maintains (including the state of each task) will be correctly restored.
  *
  * NOTE: the responses that are received must be Serializable.
  *
  * In order for the Orchestrator and the Tasks to be able to achieve all of this they have to access and modify
  * each others state directly. This means they are very tightly coupled with each other. To make this relation more
  * obvious and to enforce it, you will only be able to create tasks if you have a reference to an orchestrator
  * (which is passed implicitly to a task).
  *
  * If you have the need to refactor the creation of tasks so that you can use them in multiple orchestrators you can
  * leverage self type annotations.
  *
  * @param settings the settings to use for this orchestrator.
  * @tparam R the type of result this orchestrator returns when it finishes.
  */
sealed abstract class AbstractOrchestrator[R](val settings: Settings)
  extends PersistentActor with AtLeastOnceDelivery with ActorLogging with IdImplicits {
  import Orchestrator._
  
  /** The type of the state this orchestrator maintains. */
  type S <: State
  /** The type of Id this orchestrator handles. */
  type ID <: Id
  
  /** This exists to make the creation of FullTasks easier. */
  final implicit val orchestrator: AbstractOrchestrator[_] = this
  
  /**
    * All the tasks this orchestrator will have will be stored here, which allows them to have a stable index.
    * Once every task is added to this list, this becomes "immutable", that is, it will no longer be modified.
    * This is a Buffer because we want to ensure indexing elements is very fast. Also mutable.Buffer has about
    * half the memory overhead of a List. See the excellent blog post of Li Haoyi for more details:
    * http://www.lihaoyi.com/post/BenchmarkingScalaCollections.html#lists-vs-mutablebuffer
    */
  private[this] final val _tasks = mutable.Buffer.empty[FullTask[_, _]]
  final def tasks: immutable.Seq[FullTask[_, _]] = _tasks.toVector // To make sure the mutability does not escape
  
  /** We use a HashMap to ensure remove/insert operations are very fast O(eC). The keys are the Task.index. */
  protected[this] final var _waitingTasks = HashMap.empty[Int, Task[_]]
  final def waitingTasks: HashMap[Int, Task[_]] = _waitingTasks
  
  private[this] final var _finishedTasks = 0
  /** How many tasks of this orchestrator have successfully finished. Aborted tasks do not count as a finished task. */
  final def finishedTasks: Int = _finishedTasks
  
  // The state this orchestrator is storing. Starts out empty.
  protected final var _state: S = _
  /** Gets the state this orchestrator currently has. */
  final def state: S = _state
  
  // The id obtained in the StartOrchestrator message which prompted the execution of this orchestrator tasks
  // This is mainly used for TaskSpawnOrchestrator
  private[this] final var _startId: Long = _
  protected def startId: Long = _startId
  
  //Used to ensure inner orchestrators have different names and persistent ids
  // TODO: should this be @volatile or an AtomicInt?
  private[this] final var _innerOrchestratorsNextId: Int = 0
  protected[akkastrator] final def nextInnerOrchestratorId(): Int = {
    val id = _innerOrchestratorsNextId
    _innerOrchestratorsNextId += 1
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
    * By setting this to 0 and sending `SaveSnapshot` messages you can manually choose when to save snapshots.
    */
  def saveSnapshotRoughlyEveryXMessages: Int = settings.saveSnapshotRoughlyEveryXMessages
  def scheduleSnapshotIfNeeded(): Unit = {
    // It is lastSequenceNr % (saveSnapshotEveryXMessages * 2) because we persist TaskStarted and TaskFinished,
    // however we are only interested in TaskFinished. This will roughly correspond to every X TaskFinished.
    if (saveSnapshotRoughlyEveryXMessages > 0 && lastSequenceNr % (saveSnapshotRoughlyEveryXMessages * 2) == 0) {
      self ! SaveSnapshot
    }
  }
  
  def withLogPrefix(message: => String): String = s"[${self.path.name}] $message"
  
  /** Computes ID from the deliveryId of akka-persistence. Also updates this orchestrator state if necessary. */
  def computeID(destination: ActorPath, deliveryId: DeliveryId): ID
  /** Converts ID to the deliveryId needed for the confirmDelivery method of akka-persistence. */
  def deliveryIdOf(destination: ActorPath, id: ID): DeliveryId
  /** Ensures the received message was in fact destined to be received by `task`. */
  def matchId(task: Task[_], id: Long): Boolean
  
  
  /**
    * INTERNAL API
    * Only FullTask takes advantage of the private[akkastrator].
    */
  private[akkastrator] def addTask(task: FullTask[_, _]): Int = {
    val index = tasks.length
    _tasks += task
    // By adding the tasks without dependencies directly to _waitingTasks, we ensure that when the StartOrchestrator message
    // is received we do not need to iterate through all the tasks to compute which ones can start right away.
    if (task.dependencies == HNil) {
      _waitingTasks += index -> task.innerCreateTask()
    }
    index
  }
  
  private[akkastrator] def taskStarted(task: FullTask[_, _], innerTask: Task[_]): Unit = {
    _waitingTasks += task.index -> innerTask
    context become computeCurrentBehavior()
  }
  
  
  private[akkastrator] def taskFinished(task: FullTask[_, _]): Unit = {
    _waitingTasks -= task.index
    _finishedTasks += 1
    context become computeCurrentBehavior()
    
    scheduleSnapshotIfNeeded()
    
    onTaskFinish(task)
    
    //TODO: does invoking onFinish inside the persistHandler cause any problem?
    if (finishedTasks == tasks.size) {
      onFinish()
    }
  }
  /**
    * User overridable callback. Its called every time a task finishes.
    *
    * You can use this to implement very refined termination strategies.
    *
    * By default does nothing.
    *
    * { @see onTaskAbort} for a callback when a task aborts.
    */
  def onTaskFinish(task: FullTask[_, _]): Unit = ()
  /**
    * User overridable callback. Its called after every task finishes.
    * If a task aborts then it will prevent this method from being invoked.
    *
    * By default logs that the Orchestrator has finished then stops it.
    *
    * You can use this to implement your termination strategy.
    *
    * If a orchestrator starts without tasks it will finish right away.
    */
  def onFinish(): Unit = {
    log.info(withLogPrefix("Finished!"))
    // TODO: it would be nice to have a default Success case to send to the parent
    context stop self
  }
  
  
  private[akkastrator] def taskAborted(task: FullTask[_, _]): Unit = {
    _waitingTasks -= task.index
    // We do not increment finishedTasks because the task did not finish
    context become computeCurrentBehavior()
  }
  /**
    * User overridable callback. Its called every time a task aborts.
    *
    * You can use this to implement very refined termination strategies.
    *
    * By default aborts the orchestrator via `onAbort` with a `TaskAborted` failure.
    *
    * Note: if you invoke become/unbecome inside this method, the contract that states
    *       <cite>"Waiting tasks or tasks which do not have this task as a dependency will
    *       remain untouched"</cite> will no longer be guaranteed.
    *       If you wish to still have this guarantee you can do {{{
    *         context.become(computeCurrentBehavior() orElse yourBehavior)
    *       }}}
    *
    * { @see onTaskFinish } for a callback when a task finishes.
    *
    * @param task the task that aborted.
    */
  def onTaskAbort(task: FullTask[_, _], cause: Throwable): Unit = {
    onAbort(TaskAborted(task.report, cause, startId))
  }
  /**
    * User overridable callback. Its called when the orchestrator is aborted. By default an orchestrator
    * aborts as soon as a task aborts. However this functionality can be changed by overriding `onTaskAbort`.
    *
    * By default logs that the orchestrator has aborted, sends a message to its parent explaining why the
    * orchestrator aborted then stops it.
    *
    * You can use this to implement your termination strategy.
    */
  def onAbort(failure: Failure): Unit = {
    log.info(withLogPrefix(s"Aborted due to exception: ${failure.cause}!"))
    context.parent ! failure
    context stop self
  }
  
  
  final def recoveryAwarePersist(event: Any, logMessage: String)(handler: => Unit): Unit = {
    if (recoveryRunning) {
      // When recovering the event is already persisted no need to persist it again.
      handler
    } else {
      persist(event) { _ =>
        log.debug(withLogPrefix(logMessage))
        handler
      }
    }
  }
  
  
  final def computeCurrentBehavior(): Receive = {
    val baseCommands: Actor.Receive = alwaysAvailableCommands orElse {
      case StartTask(index) => tasks(index).start()
    }
    // baseCommands is the first receive to guarantee the messages vital to the correct working of the orchestrator
    // won't be taken first by one of the tasks behaviors or the extraCommands. Similarly extraCommands
    // is the last receive to ensure it doesn't take one of the messages of the waiting task behaviors.
    waitingTasks.values.map(_.behaviorHandlingTimeout).fold(baseCommands)(_ orElse _) orElse extraCommands
  }
  
  final def unstarted: Actor.Receive = {
    case m @ StartOrchestrator(id) =>
      recoveryAwarePersist(m, s"Persisted $m") {
        _startId = id
        log.info(withLogPrefix(s"Started with StardId = $id"))
        if (tasks.isEmpty) {
          onFinish()
        } else if (!recoveryRunning) {
          // Every task adds itself to `tasks`. If it has no dependencies then it will also be added to `waitingTasks`.
          // So in order to start the orchestrator we only need to start the tasks in the `waitingTasks`.
          // With this ruse we don't have to iterate through `tasks` in order to compute which ones are ready to start.
          // Once a task finishes it will notify every task that depend on it that it was finished. When all the dependencies
          // of a task finish a StartTask is scheduled.
          waitingTasks.values.foreach(_.start())
          // If recovery is running we don't need to start the tasks because we will eventually handle a TaskStarted
          // which will start the task(s).
        }
      }
  }
  final def alwaysAvailableCommands: Actor.Receive = {
    case Status =>
      sender() ! StatusResponse(tasks.map(_.report))
    case SaveSnapshot =>
      saveSnapshot(state)
    case ShutdownOrchestrator =>
      context stop self
  }
  
  final def receiveCommand: Actor.Receive = unstarted orElse alwaysAvailableCommands orElse extraCommands
  
  /**
    * Override this method to add extra commands that are always handled by this orchestrator (except when recovering).
    */
  def extraCommands: Actor.Receive = PartialFunction.empty[Any, Unit]
  
  def receiveRecover: Actor.Receive = unstarted orElse {
    case SnapshotOffer(_, offeredSnapshot: State) =>
      _state = offeredSnapshot.asInstanceOf[S]
    case Event.TaskStarted(taskIndex) =>
      val task = tasks(taskIndex)
      log.info(task.withOrchestratorAndTaskPrefix("Recovering TaskStarted."))
      task.start()
    case f @ Event.TaskFinished(taskIndex, _) =>
      val task: Task[_] = waitingTasks(taskIndex)
      log.info(task.withOrchestratorAndTaskPrefix("Recovering TaskFinished."))
      f.finish(task)
    case Event.TaskAborted(taskIndex, cause) =>
      val task = waitingTasks(taskIndex)
      log.info(task.withOrchestratorAndTaskPrefix("Recovering TaskAborted."))
      task.abort(cause)
    case RecoveryCompleted =>
      if (tasks.nonEmpty) {
        val tasksString = tasks.map(t => t.withTaskPrefix(t.state.toString)).mkString("\n\t")
        log.debug(withLogPrefix(s"""Recovery completed:
                     |\t$tasksString
                     |\tNumber of unconfirmed messages: $numberOfUnconfirmed
                     |\tInner orchestrator next ID: ${_innerOrchestratorsNextId}""".stripMargin))
      }
  }
}

/**
  * @inheritdoc
  *
  * In a simple orchestrator the same sequence number (of akka-persistence) is used for all the
  * destinations of the orchestrator. Because of this, ID = DeliveryId, and matchId only checks the deliveryId
  * as that will be enough information to disambiguate which task should handle the response.
  */
abstract class Orchestrator[R](settings: Settings = new Settings()) extends AbstractOrchestrator[R](settings) {
  /** This orchestrator accepts any kind of State. */
  final type S = State
  _state = EmptyState
  
  /** This orchestrator uses DeliveryId directly because the same sequence number (of the akka-persistence)
    * is used for all of its destinations. Which means the destinations of this orchestrator will see gaps
    * in the delivery ids of the received messages.*/
  final type ID = DeliveryId
  
  final def computeID(destination: ActorPath, deliveryId: DeliveryId): DeliveryId = deliveryId
  final def deliveryIdOf(destination: ActorPath, id: ID): DeliveryId = id
  final def matchId(task: Task[_], id: Long): Boolean = {
    val deliveryId: DeliveryId = id
    val matches = task.expectedID.contains(deliveryId)
    
    log.debug(task.withOrchestratorAndTaskPrefix{
      String.format(
        s"""matchId:
            |          │ DeliveryId
            |──────────┼─────────────────
            |    VALUE │ %s
            | EXPECTED │ %s
            | Matches: %s""".stripMargin,
        Some(deliveryId), task.expectedID,
        matches.toString.toUpperCase
      )
    })
    matches
  }
  
}

/**
  * @inheritdoc
  *
  * In a DistinctIdsOrchestrator an independent sequence is used for each destination of the orchestrator.
  * To be able to achieve this, an extra state is needed: the DistinctIds.
  * In this orchestrator the delivery id is not sufficient to disambiguate which task should handle the message,
  * so the ID = CorrelationId and the matchId also needs to check the sender.
  * There is also the added necessity of being able to compute the correlation id for a given (sender, deliveryId)
  * as well as translating a correlation id back to a delivery id.
  */
abstract class DistinctIdsOrchestrator[R](settings: Settings = new Settings()) extends AbstractOrchestrator[R](settings) {
  /** This orchestrator requires that the state includes DistinctIds. */
  final type S = DistinctIdsState
  _state = DistinctIdsState()
  
  /** This orchestrator uses CorrelationId for its ID. This ensures every destination sees an independent
    * sequence without gaps. */
  final type ID = CorrelationId
  
  final def computeID(destination: ActorPath, deliveryId: DeliveryId): CorrelationId = {
    val correlationId = state.nextCorrelationIdFor(destination)
    
    _state = state.updatedIdsPerDestination(destination, correlationId -> deliveryId)
    log.debug(s"State for $destination is now:\n\t" + state.idsOf(destination).mkString("\n\t"))
    
    correlationId
  }
  final def deliveryIdOf(destination: ActorPath, id: ID): DeliveryId = {
    state.deliveryIdOf(destination, id)
  }
  final def matchId(task: Task[_], id: Long): Boolean = {
    val correlationId: CorrelationId = id
    lazy val destinationPath = task.destination.toStringWithoutAddress
    lazy val (matchesSender, expectedDestination, extraInfo) = sender().path match {
      case s if s == context.system.deadLetters.path && recoveryRunning =>
        (true, context.system.deadLetters.path, s"The expected SenderPath isn't $destinationPath because recovery is running\n")
      case s if s == self.path =>
        (true, self.path, s"The expected SenderPath isn't $destinationPath because this is a Timeout\n")
      case s =>
        (s == task.destination, task.destination, "")
    }
    val matches = task.expectedID.contains(correlationId) && matchesSender
    log.debug(task.withOrchestratorAndTaskPrefix{
      val senderPathString = sender().path.toStringWithoutAddress
      val destinationString = expectedDestination.toStringWithoutAddress
      val length = senderPathString.length max destinationString.length
      String.format(
        s"""MatchId:
           |          │ %${length}s │ DeliveryId
           |──────────┼─%${length}s─┼──────────────────────────────
           |    VALUE │ %${length}s │ %s
           | EXPECTED │ %${length}s │ %s
           | %sMatches: %s""".stripMargin,
        "SenderPath", "─" * length,
        senderPathString, Some(correlationId),
        destinationString, task.expectedID,
        extraInfo, matches.toString.toUpperCase
      )
    })
    matches
  }
}