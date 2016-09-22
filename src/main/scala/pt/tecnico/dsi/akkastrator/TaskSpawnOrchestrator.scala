package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Props, Terminated}
import pt.tecnico.dsi.akkastrator.Orchestrator._

import scala.reflect.{ClassTag, classTag}

case class SpawnAndStart(props: Props, innerOrchestratorId: Int, startId: Long)

class Spawner extends Actor with ActorLogging {
  def receive: Receive = {
    case SpawnAndStart(props, innerOrchestratorId, startId) ⇒
      val innerOrchestrator = context.actorOf(props, s"inner-$innerOrchestratorId")
      
      context watch innerOrchestrator
      
      innerOrchestrator ! StartOrchestrator(startId)
      
      context become {
        case Terminated(`innerOrchestrator`) ⇒
          //This ensures we also stop the spawner when the innerOrchestrator finishes (or aborts).
          context stop self
        case msg if sender() == innerOrchestrator =>
          //The TaskSpawnOrchestrator has this spawner path as its destination, if we forwarded the response from
          //the innerOrchestrator to the parent (context.parent.forward(msg)) then the TaskSpawnOrchestrator
          //would never match the senderPath because it would be expecting a message from this spawner but it would
          //be getting it from the innerOrchestrator
          context.parent ! msg
        case msg =>
          //Here we forward the message because we want to hide the existence of this spawner from the innerOrchestrator.
          innerOrchestrator.forward(msg)
      }
  }
}

/**
  *
  * In order for this task to work correctly one of two things must happen:
  *  · The created orchestrator must send to its parent a TasksFinished and a TasksAborted when it finishes or aborts respectively.
  *  · The method behavior must be overridden to handle the messages the inner orchestrator sends when it terminates or
  *    aborts.
  *
  * @param props
  * @param description a text that describes this task in a human readable way. It will be used when the status of this
  *                    task orchestrator is requested.
  * @param dependencies the tasks that must have finished in order for this task to be able to start.
  * @param orchestrator the orchestrator upon which this task will be created.
  * @tparam R the type the AbstractOrchestrator created in Props must have as its type parameter.
  * @tparam O the type of AbstractOrchestrator the Props must create.
  */
class TaskSpawnOrchestrator[R, O <: AbstractOrchestrator[R]: ClassTag](props: Props, description: String, dependencies: Set[Task[_]] = Set.empty)
                                                                      (implicit orchestrator: AbstractOrchestrator[_])
  extends Task[R](description, dependencies)(orchestrator) {
  
  require(classTag[O].runtimeClass.isAssignableFrom(props.actorClass()),
    "Props.actorClass must comply with <: AbstractOrchestrator[R]")
  
  val innerOrchestratorId = orchestrator.nextInnerOrchestratorId()
  
  final val destination = orchestrator.context.actorOf(Props[Spawner], s"spawner-$innerOrchestratorId").path
  final def createMessage(id: Long): Any = SpawnAndStart(props, innerOrchestratorId, id)
  
  def behavior: Receive = {
    case m @ TasksFinished(result, id) if matchId(id) =>
      finish(m, id, result.asInstanceOf[R])
    case m @ TasksAborted(_, cause, id) if matchId(id) =>
      abort(m, cause, id)
  }
}