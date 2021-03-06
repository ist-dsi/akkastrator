package pt.tecnico.dsi.akkastrator

import scala.reflect.ClassTag

import akka.actor.{ActorPath, Props}
import akka.testkit.ImplicitSender
import pt.tecnico.dsi.akkastrator.DSL._
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Step1_RefactoringSpec._
import shapeless.{::, HNil}

object Step1_RefactoringSpec {
  trait SimpleTasks { self: Orchestrator[_] =>
    val theOneTask: FullTask[Unit, HNil] = FullTask("the One") createTask { _ =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        def behavior: Receive = {
          case SimpleMessage(id) if matchId(id) => finish(())
        }
      }
    }
    
    def deleteUser(user: String): FullTask[Unit, HNil] = FullTask(s"Delete user $user") createTaskWith { case HNil =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        def behavior: Receive = {
          case SimpleMessage(id) if matchId(id) => finish(())
        }
      }
    }
  }
  
  trait DistinctIdsTasks { self: DistinctIdsOrchestrator[_] =>
    val getHiggs: FullTask[String, HNil] = FullTask("find the higgs boson") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        
        def behavior: Receive = {
          case SimpleMessage(id) if matchId(id) =>
            finish("a non-zero constant value almost everywhere")
        }
      }
    }
    
    def postTask(what: String, where: String): TaskBuilder[Unit] = new Task[Unit](_) {
      val destination: ActorPath = ActorPath.fromString(s"akka://user/$what/$where")
      
      def createMessage(id: Long): Serializable = SimpleMessage(id)
      
      def behavior: Receive = {
        case SimpleMessage(id) if matchId(id) => finish(())
      }
    }
    
    def post(dependencies: FullTask[String, HNil] :: FullTask[String, HNil] :: HNil): FullTask[Unit, _] = {
      FullTask("posting", dependencies) createTaskF postTask _
    }
  }
  
  trait AbstractTasks { self: AbstractOrchestrator[_] =>
    val obtainLocation: FullTask[String, HNil] = FullTask("obtain The location") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        
        def behavior: Receive = {
          case SimpleMessage(id) if matchId(id) => finish("::1")
        }
      }
    }
    
    case class PingTask(ip: String)(ft: FullTask[Unit, _]) extends Task[Unit](ft) {
      val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
      def createMessage(id: Long): Serializable = SimpleMessage(id)
      
      def behavior: Receive = {
        case SimpleMessage(id) if matchId(id) => finish(())
      }
    }
    
    def ping(location: FullTask[String, _] :: HNil): FullTask[Unit, _] = {
      FullTask("Ping", location) createTaskF PingTask.apply _
    }
  }
  
  class Simple1Orchestrator extends Orchestrator() with SimpleTasks {
    def persistenceId: String = "Simple1"
    
    deleteUser("a")
  }
  
  class DistinctIds1Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks {
    def persistenceId: String = "DistinctIds1"
    
    val where: FullTask[String, HNil] = FullTask("get where") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        
        def behavior: Receive = {
          case SimpleMessage(id) if matchId(id) => finish("http://example.com")
        }
      }
    }
    
    val c: FullTask[Unit, _] = post(getHiggs :: where :: HNil)
    
    def post2(someParam: String)(dependencies: FullTask[Unit, _] :: FullTask[String, _] :: HNil): FullTask[String, _] = {
      FullTask("demo", dependencies) createTask { case (_, tb) =>
        new Task[String](_){
          val destination: ActorPath = ActorPath.fromString(s"akka://user/dummy/$someParam")
          def createMessage(id: Long): Serializable = SimpleMessage(id)
          
          def behavior: Receive = {
            case SimpleMessage(id) if matchId(id) => finish(tb)
          }
        }
      }
    }
    
    // Two levels nesting
    //val d = (c, where) -> post2("someValue")
  }
  
  class Simple2Orchestrator extends Orchestrator() with SimpleTasks with AbstractTasks {
    def persistenceId: String = "Simple2"
    obtainLocation isDependencyOf ping
  }
  
  class DistinctIds2Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks with AbstractTasks {
    def persistenceId: String = "DistinctIds2"
    
    (getHiggs, obtainLocation) areDependenciesOf post
  }
}

class Step1_RefactoringSpec extends ActorSysSpec with ImplicitSender {
  def testNumberOfTasks[O <: AbstractOrchestrator[_]: ClassTag](numberOfTasks: Int): Unit = {
    val orchestrator = system.actorOf(Props[O])
    orchestrator ! Status
    val tasks = expectMsgType[StatusResponse].tasks
    tasks.length shouldBe numberOfTasks
    orchestrator ! ShutdownOrchestrator
  }
  
  "An orchestrator with refactored tasks" should {
    "not typecheck" when {
      "a distinctIds orchestrator refactored tasks are added to a simple orchestrator" in {
        """class FailingOrchestrator extends Orchestrator() with DistinctIdsTasks {
          def persistenceId: String = "failing"
        }""" shouldNot typeCheck
      }
      "a simple orchestrator refactored tasks are added to a distinctIds orchestrator" in {
        """class FailingOrchestrator extends DistinctIdsOrchestrator() with SimpleTasks {
          def persistenceId: String = "failing"
        }""" shouldNot typeCheck
      }
    }
    
    "add the refactored tasks to the orchestrator" when {
      "SimpleTasks are added to a simple orchestrator" in {
        // 2 because: theOneTask and deleteUser("a")
        testNumberOfTasks[Simple1Orchestrator](numberOfTasks = 2)
      }
      "DistinctIdsTasks are added to a distinctIds orchestrator" in {
        // 4 because: getHiggs, where, c and d
        testNumberOfTasks[DistinctIds1Orchestrator](numberOfTasks = 3)
      }
      "AbstractTasks are added to a simple orchestrator" in {
        // 4 because: theOneTask, obtainLocation and the ping which depend on obtainLocation
        testNumberOfTasks[Simple2Orchestrator](numberOfTasks = 3)
      }
      "AbstractTasks are added to a distinctIds orchestrator" in {
        // 3 because: getHiggs, obtainLocation and post which depends on the previous ones
        testNumberOfTasks[DistinctIds2Orchestrator](numberOfTasks = 3)
      }
    }
  }
}
