package pt.tecnico.dsi.akkastrator

import java.util.concurrent.TimeoutException

import scala.reflect.{ClassTag, classTag}

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Props, Terminated}
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.Timeout

case class SpawnAndStart(props: Props, innerOrchestratorId: Int, startId: Long)

// This actor ensures:
//  · The inner orchestrator is only created when the task is started.
//  · The TaskSpawnOrchestrator destination has a stable path.
//  · When the outer orchestrator is recovering the inner orchestrator is not created if it already
//    had finished before the crash.
class Spawner(task: FullTask[_, _]) extends Actor with ActorLogging {
  log.debug(task.withLogPrefix(s"Created Spawner: $self"))
  
  def receive: Receive = {
    case SpawnAndStart(props, innerOrchestratorId, startId) =>
      val innerOrchestrator = context.actorOf(props, s"${props.actorClass().getSimpleName.toLowerCase}-$innerOrchestratorId")
  
      log.debug(task.withLogPrefix(s"Created inner orchestrator: $innerOrchestrator"))
      
      context watch innerOrchestrator
      
      log.debug(task.withLogPrefix(s"Sending StartOrchestrator($startId) to $innerOrchestrator"))
      innerOrchestrator ! StartOrchestrator(startId)
      
      context become innerSpawned(innerOrchestrator)
    case Timeout(_) if sender() == context.parent =>
      context stop self
  }
  
  def innerSpawned(innerOrchestrator: ActorRef): Receive = {
    case _: SpawnAndStart =>
      // If we receive a SpawnAndStart after we already have spawned the inner orchestrator
      // It means the outer task grew inpatient and is sending us its message again.
      // We purposefully ignore this message to ensure it does not fill the logs with
      // unhandled message from $parentOrchestrator: SpawnAndStart(...)
      // This way the most that will be logged will be an UnconfirmedWarning.
    case Terminated(`innerOrchestrator`) =>
      // This ensures we also stop the spawner when the innerOrchestrator finishes (or aborts).
      context stop self
    case Timeout(_) if sender() == context.parent =>
      // The outer task timed out which means it's no longer interested in the outcome of the inner orchestrator.
      // So we terminate it. This will cause some "unhandled messages" to be logged since the destinations
      // of the inner tasks might send in their responses.
      innerOrchestrator ! ShutdownOrchestrator
      context stop self
    case msg if sender() == innerOrchestrator =>
      // The TaskSpawnOrchestrator has this spawner path as its destination, if we forwarded the response from
      // the innerOrchestrator to the parent (context.parent.forward(msg)) then the TaskSpawnOrchestrator
      // would never match the senderPath because it would be expecting a message from this spawner but it would
      // be getting it from the innerOrchestrator
      context.parent ! msg
  }
}

/**
  * In order for this task to work correctly either:
  *  · The created orchestrator must send to its parent a Orchestrator.Success when it finishes and a Orchestrator.Failure
  *    when it aborts. And terminate afterwords of sending the messages.
  *  · Or the method behavior must be overridden to handle the messages the inner orchestrator sends when it terminates or
  *    aborts.
  *
  * @param props
  * @tparam R the type the to be created AbstractOrchestrator returns.
  * @tparam O the type of AbstractOrchestrator the Props must create.
  */
class TaskSpawnOrchestrator[R, O <: AbstractOrchestrator[R]: ClassTag](task: FullTask[_, _])(props: Props) extends Task[R](task) {
  // Props only imposes the restriction that the class it creates must be <: Actor.
  // However we have a more refined restriction that the class it creates must be <: AbstractOrchestrator[R]
  require(classTag[O].runtimeClass.isAssignableFrom(props.actorClass()),
    "TaskSpawnOrchestrator props.actorClass must conform to <: AbstractOrchestrator[R]")
  
  final val innerOrchestratorId: Int = orchestrator.nextInnerOrchestratorId()
  final val spawner: ActorRef = orchestrator.context.actorOf(Props(classOf[Spawner], task), s"spawner-$innerOrchestratorId")
  final val destination: ActorPath = spawner.path
  final def createMessage(id: Long): Serializable = SpawnAndStart(props, innerOrchestratorId, id)
  
  def behavior: Receive = {
    case m: Success[R] if matchId(m.id) =>
      finish(m, m.id, m.result)
    case m: Failure if matchId(m.id) =>
      abort(m, m.id, m.cause)
    case m @ Timeout(id) =>
      spawner.tell(m, orchestrator.self)
      // Make it look like the timeout is automatically handled
      abort(m, id, cause = new TimeoutException())
  }
}