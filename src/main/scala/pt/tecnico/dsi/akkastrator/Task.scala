package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorPath}
import pt.tecnico.dsi.akkastrator.Orchestrator.{SaveSnapshot, TaskReport}
import pt.tecnico.dsi.akkastrator.Task._

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case class Aborted(cause: AbortCause) extends State
  // It would be nice to find a way to ensure the type parameter R of this class matches with the type parameter R of Task
  case class Finished[R](result: R) extends State
  
  case object Timeout
}

//TODO: it would be nice to receive FullTask with the type parameter: (task: FullTask[R, _, _])
//For time being we cannot to it because it breaks the DSL

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
  */
abstract class Task[R](val task: FullTask[_, _, _]) {
  import IdImplicits._
  import task.orchestrator.log
  import task.{index, orchestrator, timeout}
  
  private[akkastrator] var expectedDeliveryId: Option[DeliveryId] = None
  private[akkastrator] var state: Task.State = Unstarted
  
  /** The ActorPath to whom this task will send the message(s). */
  val destination: ActorPath //This must be a val because the destination cannot change.
  /** The constructor of the message to be sent. */
  def createMessage(id: Long): Any
  
  /** If recovery is running just executes `handler`, otherwise persists the `event` and uses `handler` as its handler. */
  final private def recoveryAwarePersist(event: Event)(handler: => Unit): Unit = {
    if (orchestrator.recoveryRunning) {
      // When recovering the event is already persisted no need to persist it again.
      handler
    } else {
      orchestrator.persist(event) { _ =>
        log.debug(withLogPrefix(s"Persisted ${event.getClass.getSimpleName}."))
        handler
      }
    }
  }
  
  final protected[akkastrator] def start(): Unit = {
    require(state == Unstarted, "Start can only be invoked when this task is Unstarted.")
    log.info(withLogPrefix(s"Starting."))
    recoveryAwarePersist(MessageSent(task.index)) {
      orchestrator.deliver(destination) { deliveryId =>
        //First we make sure the orchestrator is ready to deal with the answers from destination.
        expectedDeliveryId = Some(deliveryId)
        state = Waiting
        orchestrator.unstartedTasks -= index
        orchestrator.waitingTasks += index -> this
        orchestrator.context become orchestrator.computeCurrentBehavior()
        
        if (!orchestrator.recoveryRunning) {
          //When we are recovering this method (the deliver handler) will be run
          //but the message won't be delivered every time so we hide the println to cause less confusion
          log.debug(withLogPrefix("Delivering message"))
        }
  
        //Schedule the timeout
        if (timeout.isFinite()) {
          orchestrator.context.system.scheduler.scheduleOnce(FiniteDuration(timeout.length, timeout.unit)) {
            //TODO: check if state == Waiting this works
            if (state == Waiting) {
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
  
  protected[akkastrator] def persistAndConfirmDelivery(receivedMessage: Serializable, id: Long)(continuation: => Unit): Unit = {
    recoveryAwarePersist(MessageReceived(task.index, receivedMessage)) {
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
    require(state == Waiting, "Finish can only be invoked when this task is Waiting.")
    log.info(withLogPrefix(s"Finishing."))
    persistAndConfirmDelivery(receivedMessage, id) {
      // First we make sure the orchestrator no longer deals with the answers from destination.
      // By removing this task behavior from the orchestrator we ensure re-sends do not cause the orchestrator to crash due to the
      // require at the top of this method. This means re-sends will cause a "unhandled message" log message.
      expectedDeliveryId = None
      state = Finished(result)
      orchestrator.waitingTasks -= index
      orchestrator.finishedTasks += 1
      orchestrator.context become orchestrator.computeCurrentBehavior()
  
      // This method is invoked whenever a task finishes, so it is a very appropriate location to place
      // the computation of whether we should perform an automatic snapshot.
      // It is lastSequenceNr % (saveSnapshotEveryXMessages * 2) because we persist MessageSent and MessageReceived,
      // however we are only interested in MessageReceived. This will roughly correspond to every X MessageReceived.
      if (orchestrator.saveSnapshotRoughlyEveryXMessages > 0 &&
        orchestrator.lastSequenceNr % (orchestrator.saveSnapshotRoughlyEveryXMessages * 2) == 0) {
        orchestrator.self ! SaveSnapshot
      }
  
      // Secondly we invoke the callback
      orchestrator.onTaskFinish(this)
  
      // Start tasks that depend on this one
      task.notifyDependents()
      
      // Or alternatively invoke onFinish if every task has finished
      if (orchestrator.finishedTasks == orchestrator.tasks.size) {
        orchestrator.onFinish()
      }
    }
  }

  /**
    * Causes this task to abort. This will have the following effects:
    *  1. This task will change its state to `Aborted`.
    *  2. Every unstarted task that depends on this one will never be started. This will happen because a task can
    *     only start if its dependencies have finished and this task did not finish.
    *  3. Waiting tasks or tasks which do not have this task as a dependency will continue to be executed, unless
    *     the orchestrator is stopped.
    *  4. The method `onFinish` will <b>never</b> be called. Similarly to the unstarted tasks, onFinish will only
    *     be invoked if all tasks have finished and this task did not finish.
    *  5. The method `onAbort` will be invoked in the orchestrator.
    *
    */
  final def abort(receivedMessage: Serializable, cause: AbortCause, id: Long): Unit = {
    require(state == Waiting, "Abort can only be invoked when this task is Waiting.")
    log.info(withLogPrefix(s"Aborting due to $cause."))
    persistAndConfirmDelivery(receivedMessage, id) {
      // First we make sure the orchestrator no longer deals with the answers from destination.
      expectedDeliveryId = None
      state = Aborted(cause)
      orchestrator.waitingTasks -= index
      orchestrator.context become orchestrator.computeCurrentBehavior()
      
      orchestrator.onAbort(task, receivedMessage, cause, orchestrator.tasks.groupBy(_.state))
  
      // Unlike finish we do NOT invoke:
      // · orchestrator.onTaskFinish(this) - thus ensuring onTaskFinish is not invoked for aborted tasks.
      // · task.notifyDependents() - thus keeping good on the promise that unstarted tasks that depend on this one will never be started.
      // . orchestrator.onFinish - this plus never calling onTaskFinish ensures that onFinish is never called when a task aborts.
    }
  }
  
  /**
    * INTERNAL API
    * @return The result of this Task. A Task will only have a result if it is finished. */
  private[akkastrator] def result: Option[R] = state match {
    case Finished(value) => Some(value.asInstanceOf[R])
    case _ => None
  }
  
  def withLogPrefix(message: => String): String = task.withLogPrefix(message)
  def toTaskReport: TaskReport[R] = task.toTaskReport.asInstanceOf[TaskReport[R]]
}