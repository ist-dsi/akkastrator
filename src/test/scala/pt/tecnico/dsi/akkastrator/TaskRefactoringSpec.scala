package pt.tecnico.dsi.akkastrator
import akka.actor.{ActorPath, Props}
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.ScalaFutures

import scala.reflect.ClassTag

class TaskRefactoringSpec extends ActorSysSpec with ScalaFutures with ImplicitSender {
  trait SimpleTasks { self: Orchestrator ⇒
    val someTask = new Task[Unit]("SomeTask") {
      val destination: ActorPath = ActorPath.fromString("akka://user/a")
      def createMessage(id: Long): Any = SimpleMessage("SomeTask", id)
  
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) ⇒
          finish(m, id, ())
      }
    }
  
    def deleteUser(user: String, dependencies: Task[_]*): Task[_] = new Task[Unit](s"Delete user $user", Set(dependencies:_*)) {
      val destination: ActorPath = ActorPath.fromString("akka://user/b")
      def createMessage(id: Long): Any = SimpleMessage(s"DELETE $user", id)
    
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) ⇒
          finish(m, id, ())
      }
    }
  }
  
  trait DistinctIdsTasks { self: DistinctIdsOrchestrator ⇒
    val anotherTask = new Task[Unit]("AnotherTask") {
      //Dummy destination
      val destination: ActorPath = ActorPath.fromString("akka://user/c")
      def createMessage(id: Long): Any = SimpleMessage("AnotherTask", id)
    
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) ⇒
          finish(m, id, ())
      }
    }
  
    def post(what: String, where: String, dependencies: Set[Task[_]] = Set.empty): Task[Unit] = {
      new Task[Unit](s"Post $what in $where", dependencies) {
        //Dummy destination
        val destination: ActorPath = ActorPath.fromString("akka://user/d")
        def createMessage(id: Long): Any = SimpleMessage(s"post $what in $where", id)
  
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, ())
        }
      }
    }
  }
  
  trait AbstractTasks { self: AbstractOrchestrator ⇒
    val theOneTask = new Task[Unit]("theOneTask") {
      //Dummy destination
      val destination: ActorPath = ActorPath.fromString("akka://user/e")
      def createMessage(id: Long): Any = SimpleMessage("TheOneTask", id)
      
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) ⇒
          finish(m, id, ())
      }
    }
  
    def ping(ip: String, dependencies: Task[_]*): Task[Unit] = new Task[Unit](s"Ping $ip", Set(dependencies:_*)) {
      type Result = Unit
      //Dummy destination
      val destination: ActorPath = ActorPath.fromString("akka://user/f")
      def createMessage(id: Long): Any = SimpleMessage(s"Ping $ip", id)
  
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) ⇒
          finish(m, id, ())
      }
    }
  }
  
  case object GetTasks
  trait TaskRefactoringControls { self: AbstractOrchestrator ⇒
    //We dont want to start the orchestrator right away
    override def startTasks(): Unit = ()
  }
  
  def testNumberOfTasks[O <: AbstractOrchestrator: ClassTag](creator: ⇒ O)(numberOfTasks: Int): Unit = {
    val orchestrator = system.actorOf(Props(creator))
    orchestrator ! Status
  
    val tasks = expectMsgClass(classOf[StatusResponse]).tasks
    tasks.length shouldBe numberOfTasks
  }
  
  "An orchestrator with refactored tasks" should {
    "not typecheck" when {
      "a distinctIds orchestrator refactored tasks are added to a simple orchestrator" in {
        """class MyFailingOrchestrator extends Orchestrator() with DistinctIdsTasks {
          def persistenceId: String = "failing"
        }""" shouldNot typeCheck
      }
      "a simples orchestrator refactored tasks are added to a distinctIds orchestrator" in {
        """class MyFailingOrchestrator extends DistinctIdsOrchestrator() with SimpleTasks {
          def persistenceId: String = "failing"
        }""" shouldNot typeCheck
      }
    }
    
    "add the refactored tasks to the orchestrator" when {
      "SimpleTasks are added to a simple orchestrator" in {
        class Simple1Orchestrator extends Orchestrator() with SimpleTasks with TaskRefactoringControls {
          def persistenceId: String = "Simple1"
          
          deleteUser("a")
          deleteUser("c", someTask)
        }
        testNumberOfTasks(new Simple1Orchestrator())(3)
      }
      "DistinctIdsTasks are added to a distinctIds orchestrator" in {
        class DistinctIds1Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks with TaskRefactoringControls {
          def persistenceId: String = "DistinctIds1"
  
          val p = post("something", "somewhere")
          post("a piece of information", "here", Set(p, anotherTask))
        }
        testNumberOfTasks(new DistinctIds1Orchestrator())(3)
      }
      "AbstractTasks are added to a simple orchestrator" in {
        class Simple2Orchestrator extends Orchestrator() with SimpleTasks with AbstractTasks with TaskRefactoringControls {
          def persistenceId: String = "Simple2"
      
          val p = ping("127.0.0.1")
          //Using TaskProxy as a dependency for a Task
          deleteUser("a", p)
        }
        testNumberOfTasks(new Simple2Orchestrator())(4)
      }
      "AbstractTasks are added to a distinctIds orchestrator" in {
        class DistinctIds2Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks with AbstractTasks with TaskRefactoringControls {
          def persistenceId: String = "DistinctIds2"
  
          //Using TaskProxy as a dependency for a Task
          val p = post("something", "somewhere", Set(theOneTask))
          //Using Task as a dependency for a TaskProxy
          ping("127.0.0.1", p)
        }
        testNumberOfTasks(new DistinctIds2Orchestrator())(4)
      }
    }
  }
  
  //TODO: create tasks that send messages or have behavior that is dependent upon the response obtained in a dependent task
  //TaskBundle has a dedicated suite
}
