package pt.tecnico.dsi.akkastrator

import scala.reflect.{ClassTag, classTag}

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Props, Terminated}
import pt.tecnico.dsi.akkastrator.Orchestrator._

case class SpawnAndStart(props: Props, innerOrchestratorId: Int, startId: Long)

class Spawner extends Actor with ActorLogging {
  def receive: Receive = {
    case SpawnAndStart(props, innerOrchestratorId, startId) =>
      val innerOrchestrator = context.actorOf(props, s"inner-$innerOrchestratorId")
  
      log.debug(s"Created innerOrchestrator:\n\t$innerOrchestrator")
      
      context watch innerOrchestrator
      
      innerOrchestrator ! StartOrchestrator(startId)
      
      context become {
        case Terminated(`innerOrchestrator`) =>
          //This ensures we also stop the spawner when the innerOrchestrator finishes (or aborts).
          context stop self
        case msg if sender() == innerOrchestrator =>
          //The TaskSpawnOrchestrator has this spawner path as its destination, if we forwarded the response from
          //the innerOrchestrator to the parent (context.parent.forward(msg)) then the TaskSpawnOrchestrator
          //would never match the senderPath because it would be expecting a message from this spawner but it would
          //be getting it from the innerOrchestrator
          context.parent ! msg
      }
  }
}

/**
  * In order for this task to work correctly one of two things must happen:
  *  · The created orchestrator must send to its parent a TasksFinished and a TasksAborted when it finishes or aborts respectively.
  *    And must terminate afterwords of sending the message.
  *  · The method behavior must be overridden to handle the messages the inner orchestrator sends when it terminates or
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
    "Props.actorClass must comply with <: AbstractOrchestrator[R]")
  
  final val innerOrchestratorId: Int = task.orchestrator.nextInnerOrchestratorId()
  final val spawner: ActorRef = task.orchestrator.context.actorOf(Props[Spawner], s"spawner-$innerOrchestratorId")
  
  final val destination: ActorPath = spawner.path
  final def createMessage(id: Long): Serializable = SpawnAndStart(props, innerOrchestratorId, id)
  
  def behavior: Receive = {
    case m: Success[R] if matchId(m.id) =>
      finish(m, m.id, m.result)
    case m: Failure if matchId(m.id) =>
      abort(m, m.id, m.cause)
  }
}