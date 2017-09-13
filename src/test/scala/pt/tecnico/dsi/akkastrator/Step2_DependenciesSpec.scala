package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.DurationInt

import akka.actor._
import akka.testkit.{TestDuration, TestProbe}
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import shapeless.{HList, HNil}

class Step2_DependenciesSpec extends ActorSysSpec {
  def NChainedTasksOrchestrator(numberOfTasks: Int): (Array[TestProbe], ActorRef) = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val destinations = Array.fill(numberOfTasks)(TestProbe())
    
    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new ControllableOrchestrator(startAndTerminateImmediately = true) {
      override def persistenceId: String = s"$numberOfTasks-chained-orchestrator"
      
      var last: FullTask[String, _ <: HList] = simpleMessageFulltask(letters(0).toString, destinations(0))
  
      import scala.language.existentials
      for (i <- 1 until numberOfTasks) {
        val current = simpleMessageFulltask(letters(i).toString, destinations(i), dependencies = last :: HNil)
        last = current
      }
    }))
    
    (destinations, orchestrator)
  }
  def testNChainedEchoTasks(numberOfTasks: Int): Unit = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val (destinations, orchestrator) = NChainedTasksOrchestrator(numberOfTasks)
    
    withOrchestratorTermination(orchestrator) {
      for (i <- 0 until numberOfTasks) {
        val message = destinations(i).expectMsgType[SimpleMessage]
        for (j <- (i + 1) until numberOfTasks) {
          destinations(j).expectNoMsg(100.millis.dilated)
        }
        destinations(i) reply SimpleMessage(message.id)
      }
    }
  }
  
  def withOrchestratorTermination(orchestrator: ActorRef)(test: => Unit): Unit = {
    watch(orchestrator)
    test
    expectTerminated(orchestrator)
  }
  
  "An Orchestrator" should {
    "Send one message, handle the response and finish" in {
      val destinationActor0 = TestProbe()
      
      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-single-task"
        simpleMessageFulltask("A", destinationActor0)
      }))
      
      withOrchestratorTermination(orchestrator) {
        val a0m = destinationActor0.expectMsgType[SimpleMessage]
        destinationActor0 reply SimpleMessage(a0m.id)
      }
    }
    "Send two messages, handle the response with the same type and finish" in {
      val destinations = Array.fill(2)(TestProbe())
      
      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-two-tasks"
        
        simpleMessageFulltask("A", destinations(0))
        simpleMessageFulltask("B", destinations(1))
      }))
      
      withOrchestratorTermination(orchestrator) {
        val a0m = destinations(0).expectMsgType[SimpleMessage]
        val a1m = destinations(1).expectMsgType[SimpleMessage]
        destinations(0) reply SimpleMessage(a0m.id)
        destinations(1) reply SimpleMessage(a1m.id)
      }
    }
    
    "Handle dependencies: A → B" in {
      testNChainedEchoTasks(numberOfTasks = 2)
    }
    "Handle dependencies: A → B → C" in {
      testNChainedEchoTasks(numberOfTasks = 3)
    }
    "Handle dependencies: A → ... → J" in {
      //We want 10 commands to ensure the command colors will repeat
      testNChainedEchoTasks(numberOfTasks = 10)
    }
    "Handle dependencies: (A, B) → C" in {
      val destinations = Array.fill(3)(TestProbe())

      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-tasks-in-T"
        val a = simpleMessageFulltask("A", destinations(0))
        val b = simpleMessageFulltask("B", destinations(1))
        simpleMessageFulltask("C", destinations(2), dependencies = a :: b :: HNil)
      }))

      withOrchestratorTermination(orchestrator) {
        val a0m = destinations(0).expectMsgType[SimpleMessage]
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(0) reply SimpleMessage(a0m.id)

        val a1m = destinations(1).expectMsgType[SimpleMessage]
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(1) reply SimpleMessage(a1m.id)

        val a2m = destinations(2).expectMsgType[SimpleMessage]
        destinations(2) reply SimpleMessage(a2m.id)
      }
    }
  }
}