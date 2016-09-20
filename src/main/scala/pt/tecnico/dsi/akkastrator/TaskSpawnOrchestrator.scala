package pt.tecnico.dsi.akkastrator

import scala.reflect.classTag
import akka.actor.Actor.Receive
import akka.actor.{Actor, Props, Terminated}
import pt.tecnico.dsi.akkastrator.Orchestrator._

import scala.reflect.ClassTag

case class SpawnAndStart(props: Props, innerOrchestratorId: Int, startId: Long)

class Spawner extends Actor {
  def receive: Receive = {
    case SpawnAndStart(props, innerOrchestratorId, startId) ⇒
      val innerOrchestrator = context.actorOf(props, s"inner-$innerOrchestratorId")
      context watch innerOrchestrator
      
      innerOrchestrator ! StartOrchestrator(startId)
      
      context become {
        //An orchestrator stops when it finishes (or aborts) so we also stop the spawner when that happens
        case Terminated(`innerOrchestrator`) ⇒
          context stop self
        case msg if sender() == innerOrchestrator =>
          context.parent.forward(msg)
        case msg =>
          innerOrchestrator.forward(msg)
      }
  }
}

/**
  *
  * In order for this task to work correctly one of two things must happen:
  *  · The created orchestrator must send a TasksFinished and a TasksAborted when it finishes or aborts respectively.
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
    case m @ TasksAborted(instigatorReport, id) if matchId(id) =>
      abort(m, id)
  }
}