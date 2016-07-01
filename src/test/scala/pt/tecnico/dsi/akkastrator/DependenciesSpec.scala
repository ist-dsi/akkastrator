package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.testkit.{TestDuration, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import scala.concurrent.duration.DurationInt

class DependenciesSpec  extends ActorSysSpec {
  "An Orchestrator" should {
    "Send one message, handle the response and finish" in {
      val destinationActor0 = TestProbe()

      val orchestrator = system.actorOf(Props(new SnapshotlessOrchestrator {
        echoTask("A", destinationActor0.ref.path)
      }))

      withOrchestratorTermination(orchestrator) { _ =>
        val a0m = destinationActor0.expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        destinationActor0.reply(SimpleMessage(a0m.id))
      }
    }
    "Send two messages, handle the response with the same type and finish" in {
      val destinations = Array.fill(2)(TestProbe())

      val orchestrator = system.actorOf(Props(new SnapshotlessOrchestrator {
        echoTask("A", destinations(0).ref.path)
        echoTask("B", destinations(1).ref.path)
      }))

      withOrchestratorTermination(orchestrator) { _ =>
        val a0m = destinations(0).expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        val a1m = destinations(1).expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        destinations(0).reply(SimpleMessage(a0m.id))
        destinations(1).reply(SimpleMessage(a1m.id))
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

      val orchestrator = system.actorOf(Props(new SnapshotlessOrchestrator {
        val a = echoTask("A", destinations(0).ref.path)
        val b = echoTask("B", destinations(1).ref.path)
        echoTask("C", destinations(2).ref.path, Set(a, b))
      }))

      withOrchestratorTermination(orchestrator) { _ =>
        val a0m = destinations(0).expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(0).reply(SimpleMessage(a0m.id))

        val a1m = destinations(1).expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        destinations(2).expectNoMsg(100.millis.dilated)
        destinations(1).reply(SimpleMessage(a1m.id))

        val a2m = destinations(2).expectMsgClass(500.millis.dilated, classOf[SimpleMessage])
        destinations(2).reply(SimpleMessage(a2m.id))
      }
    }
  }
}
