package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.testkit.TestProbe

import scala.concurrent.duration.DurationInt

class DependenciesSpec extends IntegrationSpec {
  test("Case 1: Send one message, handle the response and finish") {
    val destinationActor0 = TestProbe()

    val probe = TestProbe()
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoCommand("Zero Command", destinationActor0.ref.path, id => SimpleMessage(id))
    }))
    probe.watch(orchestrator)

    val a0m = destinationActor0.expectMsgClass(3.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))

    probe.expectMsgPF(5.seconds){ case Terminated(o) if o == orchestrator => true }
  }
  test("Case 2: Send two messages, handle the response with the same type and finish") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()

    val probe = TestProbe()
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
    }))
    probe.watch(orchestrator)

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))
    destinationActor1.reply(SimpleMessage(a1m.id))

    probe.expectMsgPF(5.seconds){ case Terminated(o) if o == orchestrator => true }
  }

  test("Case 3: Handle dependencies: Zero -> One") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()

    val probe = TestProbe()
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
    }))
    probe.watch(orchestrator)

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor1.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor1.reply(SimpleMessage(a1m.id))

    probe.expectMsgPF(15.seconds){ case Terminated(o) if o == orchestrator => true }
  }
  test("Case 4: Handle dependencies: Zero -> One -> Two") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()
    val destinationActor2 = TestProbe()

    val probe = TestProbe()
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
      echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(oneCommand))
    }))
    probe.watch(orchestrator)

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor1.expectNoMsg(2.seconds)
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(7.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor1.reply(SimpleMessage(a1m.id))

    val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor2.reply(SimpleMessage(a2m.id))

    probe.expectMsgPF(15.seconds){ case Terminated(o) if o == orchestrator => true }
  }
  test("Case 5: Handle dependencies: (Zero, One) -> Two") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()
    val destinationActor2 = TestProbe()

    val probe = TestProbe()
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
      echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(zeroCommand, oneCommand))
    }))
    probe.watch(orchestrator)

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor1.reply(SimpleMessage(a1m.id))

    val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor2.reply(SimpleMessage(a2m.id))

    probe.expectMsgPF(15.seconds){ case Terminated(o) if o == orchestrator => true }
  }
}
