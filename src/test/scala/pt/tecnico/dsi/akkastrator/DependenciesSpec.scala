package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.testkit.{TestDuration, TestProbe}
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import scala.concurrent.duration.DurationInt

class DependenciesSpec  extends ActorSysSpec {
  def NChainedTasksOrchestrator(numberOfTasks: Int): (Array[TestProbe], ActorRef) = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val destinations = Array.fill(numberOfTasks)(TestProbe())
    
    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new ControllableOrchestrator(TestProbe().ref, startAndTerminateImmediately = true) {
      override def persistenceId: String = s"$numberOfTasks-chained-orchestrator"
      
      var last = echoTask(letters(0).toString, destinations(0).ref.path)
      
      for (i <- 1 until numberOfTasks) {
        val current = echoTask(letters(i).toString, destinations(i).ref.path, Set(last))
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
        val message = destinations(i).expectMsgClass(classOf[SimpleMessage])
        for (j <- (i + 1) until numberOfTasks) {
          destinations(j).expectNoMsg(100.millis.dilated)
        }
        destinations(i).reply(SimpleMessage(s"Destination $i", message.id))
      }
    }
  }
  
  def withOrchestratorTermination(orchestrator: ActorRef)(test: => Unit): Unit = {
    val probe = TestProbe()
    probe.watch(orchestrator)
    test
    probe.expectTerminated(orchestrator)
  }
  
  "An Orchestrator" should {
    "Send one message, handle the response and finish" in {
      val destinationActor0 = TestProbe()

      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(TestProbe().ref, startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-single-task"

        echoTask("A", destinationActor0.ref.path)
      }))

      withOrchestratorTermination(orchestrator) {
        val a0m = destinationActor0.expectMsgClass(classOf[SimpleMessage])
        destinationActor0.reply(SimpleMessage("Destination 0", a0m.id))
      }
    }
    "Send two messages, handle the response with the same type and finish" in {
      val destinations = Array.fill(2)(TestProbe())

      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(TestProbe().ref, startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-two-tasks"

        echoTask("A", destinations(0).ref.path)
        echoTask("B", destinations(1).ref.path)
      }))

      withOrchestratorTermination(orchestrator) {
        val a0m = destinations(0).expectMsgClass(classOf[SimpleMessage])
        val a1m = destinations(1).expectMsgClass(classOf[SimpleMessage])
        destinations(0).reply(SimpleMessage("Destination 0", a0m.id))
        destinations(1).reply(SimpleMessage("Destination 1", a1m.id))
      }
    }

    "Handle dependencies: A -> B" in {
      testNChainedEchoTasks(numberOfTasks = 2)
    }
    "Handle dependencies: A -> B -> C" in {
      testNChainedEchoTasks(numberOfTasks = 3)
    }
    "Handle dependencies: A -> ... -> J" in {
      //We want 10 commands to ensure the command colors will repeat
      testNChainedEchoTasks(numberOfTasks = 10)
    }
    "Handle dependencies: (A, B) -> C" in {
      val destinations = Array.fill(3)(TestProbe())

      val orchestrator = system.actorOf(Props(new ControllableOrchestrator(TestProbe().ref, startAndTerminateImmediately = true) {
        override def persistenceId: String = "dependencies-tasks-in-T"
        val a = echoTask("A", destinations(0).ref.path)
        val b = echoTask("B", destinations(1).ref.path)
        echoTask("C", destinations(2).ref.path, Set(a, b))
      }))

      withOrchestratorTermination(orchestrator) {
        val a0m = destinations(0).expectMsgClass(classOf[SimpleMessage])
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(0).reply(SimpleMessage("Destination 0", a0m.id))

        val a1m = destinations(1).expectMsgClass(classOf[SimpleMessage])
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(1).reply(SimpleMessage("Destination 1", a1m.id))

        val a2m = destinations(2).expectMsgClass(classOf[SimpleMessage])
        destinations(2).reply(SimpleMessage("Destination 2", a2m.id))
      }
    }
  }
}
