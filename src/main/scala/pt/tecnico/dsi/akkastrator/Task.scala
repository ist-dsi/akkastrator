package pt.tecnico.dsi.akkastrator

import akka.actor.{Actor, ActorPath}
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

object Task {
  sealed trait State
  case object Unstarted extends State
  case object Waiting extends State
  case object Finished extends State
}

private[akkastrator] trait OrchestratorContext { orchestrator: AbstractOrchestrator ⇒
  
  /**
    * A task corresponds to sending a message to an actor, handling its response and possibly
    * mutate the internal state of the Orchestrator.
    * The answer(s) to the sent message must be handled in `behavior`.
    * `behavior` must invoke `finish` when no further processing is necessary.
    * The pattern matching inside `behavior` must invoke `matchSenderAndID` to ensure
    * the received message is in fact the one that its waiting to receive.
    * The internal state of the orchestrator might be mutated inside `behavior`.
    *
    * This class is very tightly coupled with Orchestrator and the reverse is also true.
    * Because of this you can only create instances of Task inside an orchestrator.
    */
  abstract class AbstractTask[TT <: AbstractTask[TT]](val description: String, val dependencies: Set[TT]) {
    //The index of this task in the orchestrator task list. //TODO: maybe this can be private/protected
    final val index: Int = tasks.length
  
    //These methods aren't final to allow turning of the colors
    val colors = Vector(
      Console.MAGENTA,
      Console.CYAN,
      Console.GREEN,
      Console.BLUE,
      Console.YELLOW,
      Console.RED,
      Console.WHITE
    )
    val color: String = colors(index % colors.size)
    def withLoggingPrefix(message: ⇒ String): String = f"$color[$index%02d - $description] $message${Console.RESET}"
  
    //A tasks always start in the Unstarted state and without an expectedDeliveryId (None) because
    //the fields _state and _expectedDeliveryID are initialized to those values respectively.
  
    /** The ActorPath to whom this task will send the message(s). */
    val destination: ActorPath //This must be a val because the destination cannot change
    /** The constructor of the message to be sent. */
    def createMessage(id: ID): Any
  
    /** If recovery is running just executes `handler`, otherwise persists the `event` and uses `handler` as its handler.*/
    private def recoveryAwarePersist(event: Event)(handler: ⇒ Unit): Unit = {
      if (recoveryRunning) {
        // When recovering we just want to execute the handler.
        // The event is already persisted no need to persist it again.
        handler
      } else {
        persist(event) { _ ⇒
          log.debug(withLoggingPrefix(s"Persisted ${event.getClass.getSimpleName}."))
          handler
        }
      }
    }
  
    /** If this task is in `state` then `handler` is executed, otherwise a exception will be thrown.*/
    private def ensureInState(state: Task.State, operationName: String)(handler: ⇒ Unit): Unit = state match {
      case `state` ⇒ handler
      case _ ⇒
        val message = s"$operationName can only be invoked when task is $state."
        log.error(withLoggingPrefix(message))
        throw new IllegalStateException(message)
    }
  
    /**
      * Starts the execution of this task.
      * If this task is already Waiting or Finished an exception will be thrown.
      * We first persist that the message was sent (unless the orchestrator is recovering), then we send it.
      */
    final protected[akkastrator] def start(): Unit = ensureInState(Unstarted, "Start") {
      log.info(withLoggingPrefix(s"Starting."))
      recoveryAwarePersist(MessageSent(index)) {
        //First we make sure the orchestrator is ready to deal with the answers from destination
        state = Waiting
        updateCurrentBehavior()
  
        //Then we send the message to the destination
        deliver(destination) { i ⇒
          val deliveryId: DeliveryId = i
          expectedDeliveryId = Some(deliveryId)
          val id: ID = deliveryId2ID(destination, deliveryId)
    
          if (!recoveryRunning) {
            //When we are recovering this method (the deliver handler) will be run
            //but the message won't be delivered every time so we hide the println to cause less confusion
            log.debug(withLoggingPrefix(s"Delivering message with ${id.getClass.getSimpleName} = $id."))
          }
          
          createMessage(id)
        }
      }
    }
  
    /**
      * Low-level match. It will behave differently according to the orchestrator in which this task is being created.
      * The parameter `id` is a `Long` and not a `ID` so this method can be invoked in the TaskProxy. */
    def matchId(id: Long): Boolean
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
    
    private def innerFinish(receivedMessage: Any, id: Long)(extraActions: ⇒ Unit): Unit = {
      recoveryAwarePersist(MessageReceived(index, receivedMessage)) {
        confirmDelivery(ID2DeliveryId(destination, id).self)
        state = Finished
        extraActions
      }
    }
  
    /**
      * Finishes this task, which implies:
      *
      *  1. Tasks that depend on this one will be started.
      *  2. Messages that would be handled by this task will no longer be handled.
      *
      * @param receivedMessage the message which prompted the finish.
      * @param id the id obtained from the message.
      */
    final def finish(receivedMessage: Any, id: Long): Unit = ensureInState(Waiting, "Finish") {
      log.info(withLoggingPrefix(s"Finishing."))
      innerFinish(receivedMessage, id) {
        //This starts tasks that have a dependency on this task
        self ! StartReadyTasks
        //This is invoked to remove this task behavior from the orchestrator
        //This will be performed even if the orchestrator has terminated early
        updateCurrentBehavior()
      }
    }
  
    /**
      * This will cause this task <b>orchestrator</b> to terminate early.
      *
      * An early termination will have the following effects:
      *  - This task will be finished.
      *  - Every unstarted task will be prevented from starting even if its dependencies have finished.
      *  - Tasks that are waiting will remain untouched and the orchestrator will
      *    still be prepared to handle their responses.
      *  - The method `onFinish` will <b>never</b> be called even if the only tasks needed to finish
      *    the orchestrator are already waiting and their responses are received.
      *  - The method `onEarlyTermination` will be invoked in the orchestrator.
      *
      */
    final def terminateEarly(receivedMessage: Any, id: Long): Unit = ensureInState(Waiting, "TerminateEarly") {
      log.info(withLoggingPrefix(s"Terminating Early."))
      innerFinish(receivedMessage, id) {
        //This will prevent unstarted tasks from starting and onFinished from being called.
        earlyTerminated = true
        //This is invoked to remove this task behavior from the orchestrator
        updateCurrentBehavior()
        onEarlyTermination(this.asInstanceOf[T], receivedMessage, tasks.groupBy(_.state))
      }
    }
  
    /** @return whether this task state is Unstarted and all its dependencies have finished. */
    final def canStart: Boolean = state == Unstarted && dependencies.forall(_.hasFinished)
    /** @return whether this task state is `Waiting`. */
    final def isWaiting: Boolean = state == Waiting
    /** @return whether this task state is `Finished`. */
    final def hasFinished: Boolean = state == Finished
  
    /** The TaskDescription of this task. */
    final def toTaskDescription: TaskDescription = TaskDescription(index, description, state, dependencies.map(_.index))
  
    private var _state: Task.State = Unstarted
    /** @return the current state of this Task. */
    final def state: Task.State = _state
    private def state_=(state: Task.State): Unit = {
      _state = state
      log.info(withLoggingPrefix(s"State: $state."))
    }
  
    private var _expectedDeliveryId: Option[DeliveryId] = None
    /** @return the current expected deliveryId of this Task. */
    final def expectedDeliveryId: Option[DeliveryId] = _expectedDeliveryId
    private def expectedDeliveryId_=(expectedDeliveryId: Option[DeliveryId]): Unit = {
      _expectedDeliveryId = expectedDeliveryId
    }
  
    override def toString: String =
      f"""Task [$index%02d - $description]:
         |Destination: $destination
         |State: $state""".stripMargin
  }
}