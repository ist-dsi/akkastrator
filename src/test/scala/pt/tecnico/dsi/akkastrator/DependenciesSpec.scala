package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.persistence.Recovery
import akka.testkit.TestProbe

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

class DependenciesSpec extends IntegrationSpec {
  test("Case 1: Send one message, handle the response and finish") {
    val destinationActor0 = TestProbe()

    class OneCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      echoCommand("Zero Command", destinationActor0.ref.path, id => SimpleMessage(id))
    }

    val actionId = 1L
    val (parentProxy, _) = fabricatedParent(
      Props(new OneCommandOrchestrator(SimpleMessage(actionId))),
      "OneSimpleCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))

    parentProxy.expectMsg(5.seconds, ActionFinished(actionId))
  }
  test("Case 2: Send two messages, handle the response with the same type and finish") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()

    class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
    }

    val actionId = 2L
    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(actionId))),
      "TwoSimpleCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))
    destinationActor1.reply(SimpleMessage(a1m.id))

    parentProxy.expectMsg(5.seconds, ActionFinished(actionId))
  }

  test("Case 3: Handle dependencies: Zero -> One") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()

    class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
    }

    val actionId = 3L
    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(actionId))),
      "TwoDependentCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor1.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor1.reply(SimpleMessage(a1m.id))

    parentProxy.expectMsg(15.seconds, ActionFinished(actionId))
  }
  test("Case 4: Handle dependencies: Zero -> One -> Two") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()
    val destinationActor2 = TestProbe()

    class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
      echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(oneCommand))
    }

    val actionId = 4L
    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(actionId))),
      "ThreeDependentCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor1.expectNoMsg(2.seconds)
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(7.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor1.reply(SimpleMessage(a1m.id))

    val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor2.reply(SimpleMessage(a2m.id))

    parentProxy.expectMsg(15.seconds, ActionFinished(actionId))
  }
  test("Case 5: Handle dependencies: (Zero, One) -> Two") {
    val destinationActor0 = TestProbe()
    val destinationActor1 = TestProbe()
    val destinationActor2 = TestProbe()

    class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
      val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
      echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(zeroCommand, oneCommand))
    }

    val actionId = 5L
    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(actionId))),
      "ThreeDependentCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor2.expectNoMsg(2.seconds)
    destinationActor1.reply(SimpleMessage(a1m.id))

    val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor2.reply(SimpleMessage(a2m.id))

    parentProxy.expectMsg(15.seconds, ActionFinished(actionId))
  }
}
