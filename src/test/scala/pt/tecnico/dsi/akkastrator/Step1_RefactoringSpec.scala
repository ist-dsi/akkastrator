package pt.tecnico.dsi.akkastrator

import scala.reflect.ClassTag

import akka.actor.{ActorPath, Props}
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.ScalaFutures
import pt.tecnico.dsi.akkastrator.DSL._
import pt.tecnico.dsi.akkastrator.Orchestrator._
import shapeless.{::, HNil}

class Step1_RefactoringSpec extends ActorSysSpec with ScalaFutures with ImplicitSender {
  trait SimpleTasks { self: Orchestrator[_] =>
    val theOneTask = FullTask("the One") createTaskWith { case HNil =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Any = SimpleMessage("TheOneTask", id)
      
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, ())
        }
      }
    }
  
    def deleteUser(user: String) = FullTask(s"Delete user $user") createTaskWith { case HNil =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Any = SimpleMessage(s"DELETE $user", id)
  
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, ())
        }
      }
    }
  }
  
  trait DistinctIdsTasks { self: DistinctIdsOrchestrator[_] =>
    val getHiggs = FullTask("find the higgs boson") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Any = SimpleMessage("AnotherTask", id)
  
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, "a non-zero constant value almost everywhere")
        }
      }
    }
  
    def post(dependencies: FullTask[String, HNil] :: FullTask[String, HNil] :: HNil) = {
      FullTask("posting", dependencies) createTaskWith { case what :: where :: HNil =>
        new Task[Unit](_){
          val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
          def createMessage(id: Long): Any = SimpleMessage(s"post $what in $where", id)
      
          def behavior: Receive = {
            case m @ SimpleMessage(s, id) if matchId(id) ⇒
              finish(m, id, ())
          }
        }
      }
    }
  }
  
  trait AbstractTasks { self: AbstractOrchestrator[_] =>
    val obtainLocation = FullTask("obtain The location") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Any = SimpleMessage("SomeTask", id)
      
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, "::1")
        }
      }
    }
  
    // With this its not working, can't understand why
    //  def ping(location: FullTask[String, HNil, HNil] :: HNil)
    
    def ping(location: FullTask[String, _] :: HNil) = FullTask("Ping", location) createTaskWith { case ip :: HNil =>
      new Task[Unit](_) {
        //Dummy destination
        val destination: ActorPath = ActorPath.fromString("akka://user/f")
        def createMessage(id: Long): Any = SimpleMessage(s"Ping $ip", id)
    
        def behavior: Receive = {
          case m @ SimpleMessage(s, id) if matchId(id) ⇒
            finish(m, id, ())
        }
      }
    }
  }
  
  def testNumberOfTasks[O <: AbstractOrchestrator[_]: ClassTag](creator: ⇒ O, numberOfTasks: Int): Unit = {
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
        class Simple1Orchestrator extends Orchestrator() with SimpleTasks {
          def persistenceId: String = "Simple1"
          
          deleteUser("a")
        }
        testNumberOfTasks(new Simple1Orchestrator(), numberOfTasks = 2)
      }
      "DistinctIdsTasks are added to a distinctIds orchestrator" in {
        class DistinctIds1Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks {
          def persistenceId: String = "DistinctIds1"
  
          val where = FullTask("get where") createTaskWith { case HNil =>
            new Task[String](_) {
              val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
              def createMessage(id: Long): Any = SimpleMessage("Where", id)
      
              def behavior: Receive = {
                case m @ SimpleMessage(s, id) if matchId(id) ⇒
                  finish(m, id, "http://example.com")
              }
            }
          }
  
          // One level nesting
          val c = getHiggs :: where :: HNil areDependenciesOf post
  
          // These two would NOT work
          //  def post2(dependencies: FullTask[Unit, HNil] :: FullTask[String, HNil] :: HNil)
          //  def post2(dependencies: FullTask[Unit, HNil] :: FullTask[String, _] :: HNil)
          def post2(dependencies: FullTask[Unit, _] :: FullTask[String, _] :: HNil) = {
            FullTask("demo", dependencies) createTaskWith { case ta :: tb :: HNil =>
              new Task[String](_){
                val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
                def createMessage(id: Long): Any = SimpleMessage(s"demo", id)
        
                def behavior: Receive = {
                  case m @ SimpleMessage(s, id) if matchId(id) ⇒
                    finish(m, id, "demo result")
                }
              }
            }
          }
          
          // Two levels nesting
          val d = (c, where) areDependenciesOf post2
          // This would fail
          //  d :: where :: HNil areDependenciesOf post
        }
        testNumberOfTasks(new DistinctIds1Orchestrator(), numberOfTasks = 4)
      }
      "AbstractTasks are added to a simple orchestrator" in {
        class Simple2Orchestrator extends Orchestrator() with SimpleTasks with AbstractTasks {
          def persistenceId: String = "Simple2"
      
          obtainLocation isDependencyOf ping
          obtainLocation -> ping
        }
        testNumberOfTasks(new Simple2Orchestrator(), numberOfTasks = 4)
      }
      "AbstractTasks are added to a distinctIds orchestrator" in {
        class DistinctIds2Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks with AbstractTasks {
          def persistenceId: String = "DistinctIds2"
  
          (getHiggs, obtainLocation) -> post
        }
        testNumberOfTasks(new DistinctIds2Orchestrator(), numberOfTasks = 3)
      }
    }
  }
  
  //TODO: we could also should how to refactor the creation of Tasks (as oposed to only showing refactoring of FullTasks)
}
