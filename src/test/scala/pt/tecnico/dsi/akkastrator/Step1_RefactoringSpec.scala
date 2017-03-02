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
    val theOneTask: FullTask[Unit, HNil] = FullTask("the One") createTask { _ =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage("TheOneTask", id)
      
        def behavior: Receive = {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, ())
        }
      }
    }
  
    def deleteUser(user: String): FullTask[Unit, HNil] = FullTask(s"Delete user $user") createTaskWith { case HNil =>
      new Task[Unit](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
        def createMessage(id: Long): Serializable = SimpleMessage(s"DELETE $user", id)
  
        def behavior: Receive = {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, ())
        }
      }
    }
  }
  
  trait DistinctIdsTasks { self: DistinctIdsOrchestrator[_] =>
    val getHiggs: FullTask[String, HNil] = FullTask("find the higgs boson") createTaskWith { case HNil =>
      new Task[String](_) {
        val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
  
        def createMessage(id: Long): Serializable = SimpleMessage("AnotherTask", id)
  
        def behavior: Receive = {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "a non-zero constant value almost everywhere")
        }
      }
    }
  
    def postTask(what: String, where: String): TaskBuilder[Unit] = new Task[Unit](_) {
      val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
  
      def createMessage(id: Long): Serializable = SimpleMessage(s"post $what in $where", id)
  
      def behavior: Receive = {
        case m @ SimpleMessage(_, id) if matchId(id) =>
          finish(m, id, ())
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
        def createMessage(id: Long): Serializable = SimpleMessage("SomeTask", id)
      
        def behavior: Receive = {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "::1")
        }
      }
    }
  
    case class PingTask(ip: String)(ft: FullTask[_, _]) extends Task[Unit](ft) {
      //Dummy destination
      val destination: ActorPath = ActorPath.fromString("akka://user/f")
      def createMessage(id: Long): Serializable = SimpleMessage(s"Ping $ip", id)
  
      def behavior: Receive = {
        case m @ SimpleMessage(s, id) if matchId(id) =>
          finish(m, id, ())
      }
    }
    
    // Since we are always using ping where the location is a FullTask[String, HNil] this would also work:
    //  def ping(location: FullTask[String, HNil] :: HNil)
    def ping(location: FullTask[String, _] :: HNil): FullTask[Unit, _] = {
      FullTask("Ping", location) createTaskF PingTask.apply _
    }
  }
  
  def testNumberOfTasks[O <: AbstractOrchestrator[_]: ClassTag](creator: => O, numberOfTasks: Int): Unit = {
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
        // 2 because: theOneTask and deleteUser("a")
        testNumberOfTasks(new Simple1Orchestrator(), numberOfTasks = 2)
      }
      "DistinctIdsTasks are added to a distinctIds orchestrator" in {
        class DistinctIds1Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks {
          def persistenceId: String = "DistinctIds1"
          
          val where: FullTask[String, HNil] = FullTask("get where") createTaskWith { case HNil =>
            new Task[String](_) {
              val destination: ActorPath = ActorPath.fromString("akka://user/dummy")
              def createMessage(id: Long): Serializable = SimpleMessage("Where", id)
      
              def behavior: Receive = {
                case m @ SimpleMessage(_, id) if matchId(id) =>
                  finish(m, id, "http://example.com")
              }
            }
          }
          
          val c: FullTask[Unit, _] = post(getHiggs :: where :: HNil)
          
          def post2(someParam: String)(dependencies: FullTask[Unit, _] :: FullTask[String, _] :: HNil): FullTask[String, _] = {
            
            FullTask("demo", dependencies) createTask { case (ta, tb: String) =>
              new Task[String](_){
                val destination: ActorPath = ActorPath.fromString(s"akka://user/dummy/$someParam")
                def createMessage(id: Long): Serializable = SimpleMessage(tb.substring(4), id)
        
                def behavior: Receive = {
                  case m @ SimpleMessage(_, id) if matchId(id) =>
                    finish(m, id, "demo result")
                }
              }
            }
          }
          
          // Two levels nesting
          //val d = (c, where) -> post2("someValue")
        }
        // 4 because: getHiggs, where, c and d
        testNumberOfTasks(new DistinctIds1Orchestrator(), numberOfTasks = 3)
      }
      "AbstractTasks are added to a simple orchestrator" in {
        class Simple2Orchestrator extends Orchestrator() with SimpleTasks with AbstractTasks {
          def persistenceId: String = "Simple2"
          obtainLocation -> ping
          obtainLocation isDependencyOf ping
        }
        // 4 because: theOneTask, obtainLocation and the 2 pings which depend on obtainLocation
        testNumberOfTasks(new Simple2Orchestrator(), numberOfTasks = 4)
      }
      "AbstractTasks are added to a distinctIds orchestrator" in {
        class DistinctIds2Orchestrator extends DistinctIdsOrchestrator() with DistinctIdsTasks with AbstractTasks {
          def persistenceId: String = "DistinctIds2"
  
          (getHiggs, obtainLocation) -> post
        }
        // 3 because: getHiggs, obtainLocation and post which depends on the previous ones
        testNumberOfTasks(new DistinctIds2Orchestrator(), numberOfTasks = 3)
      }
    }
  }
}
