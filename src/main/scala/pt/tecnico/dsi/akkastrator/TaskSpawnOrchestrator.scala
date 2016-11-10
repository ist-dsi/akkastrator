package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Props, Terminated}
import pt.tecnico.dsi.akkastrator.Orchestrator._

import scala.reflect.ClassTag

case class SpawnAndStart(props: Props, innerOrchestratorId: Int, startId: Long)

class Spawner extends Actor with ActorLogging {
  def receive: Receive = {
    case SpawnAndStart(props, innerOrchestratorId, startId) =>
      val innerOrchestrator = context.actorOf(props, s"inner-$innerOrchestratorId")
  
      log.debug(s"Created innerOrchestrator:\n\t$innerOrchestrator")
      
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
  * In order for this task to work correctly one of two things must happen:
  *  · The created orchestrator must send to its parent a TasksFinished and a TasksAborted when it finishes or aborts respectively.
  *    And must terminate afterwords of sending the message.
  *  · The method behavior must be overridden to handle the messages the inner orchestrator sends when it terminates or
  *    aborts.
  *
  * @param props
  * @tparam R the type the AbstractOrchestrator created in Props must have as its type parameter.
  * @tparam O the type of AbstractOrchestrator the Props must create.
  */
class TaskSpawnOrchestrator[R, O <: AbstractOrchestrator[R]](props: Props, task: FullTask[_, _])
                                                            (implicit classtag: ClassTag[O]) extends Task[R](task) {
  //TODO: does require cause a crash loop? Maybe we should move this check to spawner
  require(classtag.runtimeClass.isAssignableFrom(props.actorClass()),
    "Props.actorClass must comply with <: AbstractOrchestrator[R]")
  
  val innerOrchestratorId = task.orchestrator.nextInnerOrchestratorId()
  final val spawner = task.orchestrator.context.actorOf(Props[Spawner], s"spawner-$innerOrchestratorId")
  
  final val destination = spawner.path
  final def createMessage(id: Long): Any = SpawnAndStart(props, innerOrchestratorId, id)
  
  def behavior: Receive = {
    case m @ TasksFinished(result, id) if matchId(id) =>
      finish(m, id, result.asInstanceOf[R])
    case m @ TaskAborted(_, cause, id) if matchId(id) =>
      abort(m, id, cause)
  }
}