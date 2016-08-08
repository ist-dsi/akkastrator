package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.{ActorPath, Props}

case class CreateAndStartTasks[R](collection: Seq[R], id: Long)
case class TasksFinished[R](results: Seq[R], id: Long)

/**
  * Task that spawns an orchestrator
  * How to prevent the orchestrator from starting right away.
  * How will we know the orchestrator will send a response?
  * How to handle the response?
  */

/*
trait QuorumFunction extends (Int ⇒ Int)
object Majority extends QuorumFunction {
  def apply(collectionSize: Int): Int = Math.ceil(collectionSize / 2.0).toInt
}
class AtLeast(x: Int) extends QuorumFunction {
  def apply(collectionSize: Int): Int = Math.min(x, collectionSize)
}

/** TaskQuorum
  *   Variable number of tasks
  *   Different destinations
  *   Fixed message
  *
  * TODO: Implement quorum logic:
  *   · At least X responses where X <= #destinations
  *   · Majority: X > Math.ceil(#destinations / 2)
  *  By default at least X = #destinations
  *
  *  Implementing this requires a timeout
  */
abstract class TaskQuorum[R](collection: ⇒ Seq[ActorPath], quorumFunction: QuorumFunction, description: String, dependencies: Set[Task[_]] = Set.empty)
                               (implicit orchestrator: AbstractOrchestrator)
  extends Task[Seq[R]](description, dependencies)(orchestrator) { quorum ⇒
  
  def toTask(destination: ActorPath)(implicit orchestrator: AbstractOrchestrator): Task[R]
  
  /**
    * Having 3 tasks and a Majority quorum function.
    * What to do when two tasks finish?
    *   · Invoke onFinish?
    *   · Change the third task state to Aborted?
    *
    * How to count how many tasks have finished.
    *
    * What to do when a task of the inner orchestrator aborts?
    * abort the inner orchestrator and consequently the TaskQuorum?
    */
  
  class InnerOrchestrator extends Orchestrator(orchestrator.settings) {
    def persistenceId: String = quorum.orchestrator.persistenceId + "-" + self.path.name
    
    override def withLoggingPrefix(task: Task[_], message: ⇒ String): String = {
      quorum.orchestrator.withLoggingPrefix(quorum, super.withLoggingPrefix(task, message))
    }
    
    //Prevent the orchestrator from starting immediately
    override def startTasks(): Unit = ()
    
    //We need to save the creationId so we can return it back to the context.parent when we finish
    private[this] var creationId: Long = _
    override def extraCommands: Receive = {
      // Tasks can already exist because they were created in the recover
      // We want to be careful so we don't recreate them
      case m @ CreateAndStartTasks(coll, id) if tasks.isEmpty =>
        creationId = id
        coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
        
        //TODO: since require throw an exception it might cause a crash cycle
        require(coll.nonEmpty, "Collection must have at least one element.")
        val firstDestination = tasks.head.destination
        require(tasks.forall(_.destination == firstDestination), "toTask must generate tasks with the same destination.")
        require(tasks.forall(_.dependencies.isEmpty), "toTask must generate tasks without dependencies.")
        
        persist(m) { _ ⇒
          self ! StartReadyTasks
        }
    }
    
    override def onFinish(): Unit = {
      val results = tasks.map(_.result.get.asInstanceOf[R])
      context.parent ! TasksFinished(results, creationId)
      super.onFinish()
    }
    
    //FIXME: we are not handling the onEarlyTermination case. Is it even possible?
    
    override def receiveRecover: Receive = super.receiveRecover orElse {
      case CreateAndStartTasks(coll, id) =>
        creationId = id
        coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
        self ! StartReadyTasks
    }
  }
  
  val innerOrchestratorId = orchestrator.nextInnerOrchestratorId()
  final val destination = orchestrator.context.actorOf(Props(new InnerOrchestrator), s"task-bundle-$innerOrchestratorId").path
  final def createMessage(id: Long): Any = CreateAndStartTasks(collection, id)
  final def behavior: Receive = {
    case m @ TasksFinished(results, id) if matchId(id) =>
      finish(m, id, results.asInstanceOf[Seq[R]])
  }
}
*/

/**
  * TaskBundle:
  *   Variable number of tasks
  *   Fixed destination
  *   Different messages
  */
abstract class TaskBundle[I, R](collection: ⇒ Seq[I], description: String, dependencies: Set[Task[_]] = Set.empty)
                            (implicit orchestrator: AbstractOrchestrator)
  extends Task[Seq[R]](description, dependencies)(orchestrator) { bundle ⇒
  
  def toTask(input: I)(implicit orchestrator: AbstractOrchestrator): Task[R]
  
  /**
    * TODO: what to do when a task of the inner orchestrator aborts?
    * abort the inner orchestrator and consequently the TaskBundle?
    */
  
  class InnerOrchestrator extends Orchestrator(orchestrator.settings) {
    def persistenceId: String = bundle.orchestrator.persistenceId + "-" + self.path.name
    
    override def withLoggingPrefix(task: Task[_], message: ⇒ String): String = {
      bundle.orchestrator.withLoggingPrefix(bundle, super.withLoggingPrefix(task, message))
    }
  
    //Prevent the orchestrator from starting immediately
    override def startTasks(): Unit = ()
  
    //We need to save the creationId so we can return it back to the context.parent when we finish
    private[this] var creationId: Long = _
    override def extraCommands: Receive = {
      // Tasks can already exist because they were created in the recover
      // We want to be careful so we don't recreate them
      case m @ CreateAndStartTasks(coll, id) if tasks.isEmpty =>
        creationId = id
        coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
  
        //TODO: since require throw an exception it might cause a crash cycle
        require(coll.nonEmpty, "Collection must have at least one element.")
        val firstDestination = tasks.head.destination
        require(tasks.forall(_.destination == firstDestination), "toTask must generate tasks with the same destination.")
        require(tasks.forall(_.dependencies.isEmpty), "toTask must generate tasks without dependencies.")
        
        persist(m) { _ ⇒
          self ! StartReadyTasks
        }
    }
    
    override def onFinish(): Unit = {
      val results = tasks.map(_.result.get.asInstanceOf[R])
      context.parent ! TasksFinished(results, creationId)
      super.onFinish()
    }
  
    override def receiveRecover: Receive = super.receiveRecover orElse {
      case CreateAndStartTasks(coll, id) =>
        creationId = id
        coll.asInstanceOf[Seq[I]].map(i ⇒ toTask(i)(this))
        self ! StartReadyTasks
    }
  }
  
  val innerOrchestratorId = orchestrator.nextInnerOrchestratorId()
  final val destination = orchestrator.context.actorOf(Props(new InnerOrchestrator), s"task-bundle-$innerOrchestratorId").path
  final def createMessage(id: Long): Any = CreateAndStartTasks(collection, id)
  final def behavior: Receive = {
    case m @ TasksFinished(results, id) if matchId(id) =>
      finish(m, id, results.asInstanceOf[Seq[R]])
  }
}
