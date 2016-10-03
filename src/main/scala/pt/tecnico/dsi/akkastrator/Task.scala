package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorPath}
import pt.tecnico.dsi.akkastrator.Task._

import scala.concurrent.duration.{Duration, FiniteDuration}

// Task is a stateful class, so we cannot pass it as a response to Status queries. That why there is the class TaskReport

/** An immutable representation (a report) of a Task in a given moment of time. */
case class TaskReport(description: String, dependencies: Set[Int], destination: ActorPath, state: Task.State)

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case class Aborted(cause: AbortCause) extends State
  case class Finished[R](result: R) extends State
  
  case object Timeout
}

/*
!! vs ?! vs !?

askness here is misleading because Task does not return a future, but it does have a timeout. It could have a
method that would return a future, however it might lead the user to implement certain types of code that would
easily become incompatible with Task.

!! - conveys at-least-onceness but not askness.
?! - conveys askness with emphasis (at-least-onceness but is not so obvious as !!).
     Has the advantage that an UTF-8 character already exists for it: ⁈ (however scala does not allow it as a method name)
!!? - conveys at-least-onceness as well as the askness
!? - conveys at-least-onceness and askness, both poorly.
     Has the advantage that an UTF-8 character already exists for it: ⁉ (however scala does not allow it as a method name)
     Has the advantage that is smaller than !!?.

What is a reasonable verb to describe it?

kerberos !! (Kerberos.addPrincipal("", _)) withBehavior {
  case m @ Success(id) if matchId(id) =>
} withTimeout 2.seconds

How to implement the above while guaranteeing the task isn't added twice, maybe the upsertTask will solve this problem
*/

/**
  * A task corresponds to sending a message to an actor, handling its response and possibly
  * mutate the internal state of the Orchestrator.
  *
  * The answer(s) to the sent message must be handled in `behavior`. `behavior` must invoke `finish` when
  * no further processing is necessary. Or `abort` if the received message will prevent subsequent
  * tasks from executing properly.
  *
  * The pattern matching inside `behavior` must invoke `matchId` to ensure the received message
  * is in fact the one that this task its waiting to receive.
  *
  * The internal state of the orchestrator might be mutated inside `behavior`.
  *
  * This class is very tightly coupled with Orchestrator and the reverse is also true.
  * Because of this you need to pass an instance of orchestrator.
  * Because of this you can only create instances of Task inside an orchestrator.
  *
  * @param description a text that describes this task in a human readable way. It will be used when the status of this
  *                    task orchestrator is requested.
  * @param dependencies the tasks that must have finished in order for this task to be able to start.
  * @param timeout         NOTE: the timeout does not survive restarts!
  * @param orchestrator the orchestrator upon which this task will be added and ran.
  */
abstract class Task[R](val description: String, val dependencies: Set[Task[_]] = Set.empty, timeout: Duration = Duration.Inf)
                      (implicit orchestrator: AbstractOrchestrator[_]) {
  import IdImplicits._
  import orchestrator.log
  
  
  //This way of implementing Task is very fragile, mainly due to index being computed by adding the task to the orchestrator.
  
  //The index of this task in the orchestrator task list.
  final val index: Int = orchestrator.upsertTask(this)
  
  private[akkastrator] var expectedDeliveryId: Option[DeliveryId] = None
  
  //These methods aren't final to allow turning off the colors or customizing the logging prefix.
  val taskColors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.WHITE
  )
  val color: String = taskColors(index % taskColors.size)
  def withLoggingPrefix(message: ⇒ String): String = {
    f"$color[$description] $message${Console.RESET}"
  }
  
  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change.
  /** The constructor of the message to be sent. */
  def createMessage(id: Long): Any
  
  /** If recovery is running just executes `handler`, otherwise persists the `event` and uses `handler` as its handler. */
  final private def recoveryAwarePersist(event: Event)(handler: ⇒ Unit): Unit = {
    if (orchestrator.recoveryRunning) {
      // When recovering the event is already persisted no need to persist it again.
      handler
    } else {
      orchestrator.persist(event) { _ ⇒
        log.debug(withLoggingPrefix(s"Persisted ${event.getClass.getSimpleName}."))
        handler
      }
    }
  }
  
  //TODO: timeout
  //in start scheduleOnce(timeout)(TaskTimedOut(index, deliveryId))
  //in the orchestratorCommand when TaskTimedOut is received:
  //  if (tasks(index).behavior.isDefinedAt(Timeout)) {
  //    tasks(index).behavior(Timeout)
  //  } else {
  //    Abort the task? Create the state TimedOut? What would that state mean?
  //  }
  //Is timeouts a good idea? it would easily lead to conditional logic. Could we start tasks/orchestrator only
  //when a timeout happens? Or aborting the task would be a better approach?
  
  final protected[akkastrator] def start(): Unit = {
    require(canStart, "Start can only be invoked when this task is Unstarted and all of its dependencies have finished.")
    log.info(withLoggingPrefix(s"Starting."))
    recoveryAwarePersist(MessageSent(index)) {
      orchestrator.deliver(destination) { deliveryId ⇒
        //First we make sure the orchestrator is ready to deal with the answers from destination
        expectedDeliveryId = Some(deliveryId)
        state = Task.Waiting
  
        if (!orchestrator.recoveryRunning) {
          //When we are recovering this method (the deliver handler) will be run
          //but the message won't be delivered every time so we hide the println to cause less confusion
          log.debug(withLoggingPrefix("Delivering message"))
        }
  
        //Schedule the timeout
        if (timeout.isFinite()) {
          orchestrator.context.system.scheduler.scheduleOnce(FiniteDuration(timeout.length, timeout.unit)) {
            if (isWaiting) {
              behavior.applyOrElse(Timeout, { _: Timeout.type =>
                //behavior does not handle timeout. So we abort it.
                //We know get will work because the task is waiting.
                val id = orchestrator.deliveryId2ID(destination, expectedDeliveryId.get)
                abort(receivedMessage = Timeout, cause = TimedOut, id = id.self)
              })
            }
          }(orchestrator.context.system.dispatcher)
        }
        
        val id = orchestrator.deliveryId2ID(destination, deliveryId)
        createMessage(id.self)
      }
    }
  }
  
  final def matchId(id: Long): Boolean = orchestrator.matchId(this, id)

  /**
    * The behavior of this task. This is akin to the receive method of an actor, except for the fact that an
    * all catching pattern match will cause the orchestrator to fail. For example:
    * {{{
    *   def behavior = Receive {
    *     case m => //Some code
    *   }
    * }}}
    * This will cause the orchestrator to fail because the messages won't be handled by the correct tasks.
    */
  def behavior: Actor.Receive
  
  protected[akkastrator] def persistAndConfirmDelivery(receivedMessage: Serializable, id: Long)(continuation: ⇒ Unit): Unit = {
    recoveryAwarePersist(MessageReceived(index, receivedMessage)) {
      val deliveryId = orchestrator.ID2DeliveryId(destination, id).self
      orchestrator.confirmDelivery(deliveryId)
      continuation
    }
  }

  /**
    * Finishes this task, which implies:
    *
    *  1. Tasks that depend on this one will be started.
    *  2. Messages that would be handled by this task will no longer be handled.
    *
    *  Finishing an already finished task will throw an exception.
    *
    * @param receivedMessage the message which prompted the finish.
    * @param id the id obtained from the message.
    */
  final def finish(receivedMessage: Serializable, id: Long, result: R): Unit = {
    require(isWaiting, "Finish can only be invoked when this task is Waiting.")
    log.info(withLoggingPrefix(s"Finishing."))
    persistAndConfirmDelivery(receivedMessage, id) {
      expectedDeliveryId = None
  
      // This also removes this task behavior from the orchestrator.
      // This semantic is very helpful as it ensures re-sends do not cause the orchestrator to crash due to the
      // require at the top of this method. This means re-sends will cause a "unhandled message" log message.
      state = Task.Finished(result)
      
      orchestrator.onTaskFinish(this)
      orchestrator.self ! orchestrator.StartReadyTasks
    }
  }

  /**
    * Causes this task to abort. This will have the following effects:
    *  1. This task will change its state to `Aborted`.
    *  2. Every unstarted task that depends on this one will never be started. This will happen because a task can
    *     only start if its dependencies have finished and this task did not finish.
    *  3. Waiting tasks will be untouched and the orchestrator will still be prepared to handle their responses.
    *  4. The method `onFinish` will <b>never</b> be called. Similarly to the unstarted tasks, onFinish will only
    *     be invoked if all tasks have finished and this task did not finish.
    *  5. The method `onAbort` will be invoked in the orchestrator.
    *
    */
  final def abort(receivedMessage: Serializable, cause: AbortCause, id: Long): Unit = {
    require(isWaiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withLoggingPrefix(s"Aborting due to $cause."))
    persistAndConfirmDelivery(receivedMessage, id) {
      expectedDeliveryId = None
      //This will prevent:
      // · Unstarted tasks, that depend on this one, from starting because canStart on those tasks will never return true
      // · onFinished from being called because the condition `tasks.forall(_.hasFinished)` on the orchestrator will never return true
      // It will also cause this task behavior to be removed from the orchestrator since this task will no longer be waiting.
      state = Task.Aborted(cause)
      orchestrator.onAbort(this, receivedMessage, cause, orchestrator.tasks.groupBy(_.state))
    }
  }

  /** The immutable TaskReport of this task. */
  final def toTaskReport: TaskReport = TaskReport(description, dependencies.map(_.index), destination, state)
  
  private var _state: Task.State = Unstarted
  
  /** @return The result of this Task. A Task will only have a result if it is finished. */
  final def result: Option[R] = state match {
    case Finished(r) ⇒ Some(r.asInstanceOf[R])
    case _ ⇒ None
  }
  
  /** @return whether this command state is Unstarted and all its dependencies have finished. */
  final def canStart: Boolean = state == Unstarted && dependencies.forall(_.hasFinished)
  final def isWaiting: Boolean = state == Waiting
  final def hasAborted: Boolean = state.isInstanceOf[Aborted]
  final def hasFinished: Boolean = state.isInstanceOf[Finished[_]]
  
  /** @return the current state of this Task. */
  final def state: Task.State = _state
  private[akkastrator] def state_=(newState: Task.State): Unit = {
    _state = newState
    orchestrator.context become orchestrator.computeCurrentBehavior()
  }
  
  override def toString: String =
    f"""Task [$index%02d - $description]:
       |Destination: $destination
       |State: $state""".stripMargin

  def canEqual(other: Any): Boolean = other.isInstanceOf[Task[R]]
  
  // Both equals and hashCode must be final because otherwise we cannot guarantee
  // the orchestrator.upsertTask will work correctly
  
  override final def equals(other: Any): Boolean = other match {
    case that: Task[_] ⇒
      (that canEqual this) &&
        index == that.index &&
        destination == that.destination &&
        description == that.description &&
        dependencies == that.dependencies
    case _ ⇒ false
  }
  
  override final def hashCode(): Int = {
    val state: Seq[Any] = Seq(index, destination, description, dependencies)
    state.map(_.hashCode()).foldLeft(0)((a, b) ⇒ 31 * a + b)
  }
}