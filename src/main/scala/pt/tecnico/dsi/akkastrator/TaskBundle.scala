package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.Props

case class CreateAndStartTasks[R](collection: Seq[R], id: Long)
case class TasksFinished[R](results: Seq[R], id: Long)

abstract class TaskBundle[I](collection: ⇒ Seq[I], description: String, dependencies: Set[Task] = Set.empty)
                            (implicit orchestrator: AbstractOrchestrator)
  extends Task(description, dependencies)(orchestrator) {
  
  type InnerResult
  final type Result = Seq[InnerResult]
  
  def toTask(input: I): AbstractOrchestrator ⇒ Task {type Result <: InnerResult}
  
  private[this] val outsideOrchestratorPersistenceId = orchestrator.persistenceId
  class InnerOrchestrator extends Orchestrator {
    def persistenceId: String = outsideOrchestratorPersistenceId + "-" + self.path.name
  
    private[this] var creationId: Long = _
  
    //Prevent the orchestrator from starting immediately
    override def startTasks(): Unit = ()
  
    override def extraCommands: Receive = {
      case m @ CreateAndStartTasks(coll, id) =>
        // Tasks can already exist because they were created in the recover
        // We want to be careful so we don't recreate them
        if (tasks.isEmpty) {
          creationId = id
          coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
      
          require(coll.nonEmpty, "Collection must have at least one element.")
          val firstDestination = tasks.head.destination
          require(tasks.forall(_.destination == firstDestination),
            "toTask must generate tasks with the same destination.")
      
          persist(m) { _ ⇒
            self ! StartReadyTasks
          }
        } else {
          //If we already have tasks we only want to start them once we receive the CreateAndStartTasks
          self ! StartReadyTasks
        }
    }
    
    override def onFinish() = {
      val results = tasks.map(_.result.get.asInstanceOf[InnerResult])
      context.parent ! TasksFinished(results, creationId)
      super.onFinish()
    }
  
    //FIXME: we are not handling the onEarlyTermination case. Is it even possible?
  
    override def receiveRecover: Receive = super.receiveRecover orElse {
      case CreateAndStartTasks(coll, id) =>
        creationId = id
        coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
    }
  }
  
  //TODO: create counter in AbstractOrchestrator and use it in the name of task bundle
  val taskBundleId = orchestrator.taskBundleIdCounter.getAndIncrement()
  final val destination = orchestrator.context.actorOf(Props(new InnerOrchestrator), s"task-bundle-$taskBundleId").path
  final def createMessage(id: Long): Any = CreateAndStartTasks(collection, id)
  
  final def behavior: Receive = {
    case m @ TasksFinished(results, id) if matchId(id) =>
      finish(m, id, results.asInstanceOf[Seq[InnerResult]])
  }
}
