package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration.DurationInt

class DependenciesSpec extends IntegrationSpec {
  test("Case 1: Send one message, handle the response and finish") {
    val destinationActor0 = TestProbe()

    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoTask("A", destinationActor0.ref.path)
    }))

    withOrchestratorTermination(orchestrator) { _ =>
      val a0m = destinationActor0.expectMsgClass(500.millis, classOf[SimpleMessage])
      destinationActor0.reply(SimpleMessage(a0m.id))
    }
  }
  test("Case 2: Send two messages, handle the response with the same type and finish") {
    val destinations = Array.fill(2)(TestProbe())

    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoTask("A", destinations(0).ref.path)
      echoTask("B", destinations(1).ref.path)
    }))

    withOrchestratorTermination(orchestrator) { _ =>
      val a0m = destinations(0).expectMsgClass(500.millis, classOf[SimpleMessage])
      val a1m = destinations(1).expectMsgClass(500.millis, classOf[SimpleMessage])
      destinations(0).reply(SimpleMessage(a0m.id))
      destinations(1).reply(SimpleMessage(a1m.id))
    }
  }

  test("Case 3: Handle dependencies: A -> B") {
    testNChainedEchoTasks(numberOfTasks = 2)
  }
  test("Case 4: Handle dependencies: A -> B -> C") {
    testNChainedEchoTasks(numberOfTasks = 3)
  }
  test("Case 5: Handle dependencies: A -> ... -> J") {
    //We want 10 commands to ensure the command colors will repeat
    testNChainedEchoTasks(numberOfTasks = 10)
  }
  test("Case 6: Handle dependencies: (A, B) -> C") {
    val destinations = Array.fill(3)(TestProbe())

    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      val a = echoTask("A", destinations(0).ref.path)
      val b = echoTask("B", destinations(1).ref.path)
      echoTask("C", destinations(2).ref.path, Set(a, b))
    }))

    withOrchestratorTermination(orchestrator) { _ =>
      val a0m = destinations(0).expectMsgClass(500.millis, classOf[SimpleMessage])
      destinations(2).expectNoMsg(100.millis)
      destinations(0).reply(SimpleMessage(a0m.id))

      val a1m = destinations(1).expectMsgClass(500.millis, classOf[SimpleMessage])
      destinations(2).expectNoMsg(100.millis)
      destinations(1).reply(SimpleMessage(a1m.id))

      val a2m = destinations(2).expectMsgClass(500.millis, classOf[SimpleMessage])
      destinations(2).reply(SimpleMessage(a2m.id))
    }
  }
}
