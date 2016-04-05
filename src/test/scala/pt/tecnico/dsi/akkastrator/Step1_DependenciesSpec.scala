package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.persistence.Recovery
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

class Step1_DependenciesSpec extends TestKit(ActorSystem("Orchestrator", ConfigFactory.load()))
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with TestUtils {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("Case 1: Send one message, handle the response and finish") {
    val destinationActor0 = TestProbe()

    class OneCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
      //No state, snapshots and recovery
      var state = Orchestrator.EmptyState
      val saveSnapshotInterval: FiniteDuration = Duration.Zero
      override def preStart(): Unit = self ! Recovery(toSequenceNr = 0L)

      echoCommand("Zero Command", destinationActor0.ref.path, id => SimpleMessage(id))
    }

    val (parentProxy, _) = fabricatedParent(
      Props(new OneCommandOrchestrator(SimpleMessage(id = 1L))),
      "OneSimpleCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))

    parentProxy.expectMsg(5.seconds, ActionFinished(id = 1L))
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

    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(id = 2L))),
      "TwoSimpleCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor0.reply(SimpleMessage(a0m.id))
    destinationActor1.reply(SimpleMessage(a1m.id))

    parentProxy.expectMsg(5.seconds, ActionFinished(id = 2L))
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

    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(id = 3L))),
      "TwoDependentCommandOrchestrator"
    )

    val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
    destinationActor1.expectNoMsg(2.seconds)
    destinationActor0.reply(SimpleMessage(a0m.id))

    val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
    destinationActor1.reply(SimpleMessage(a1m.id))

    parentProxy.expectMsg(15.seconds, ActionFinished(id = 3L))
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

    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(id = 4L))),
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

    parentProxy.expectMsg(15.seconds, ActionFinished(id = 4L))
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

    val (parentProxy, _) = fabricatedParent(
      Props(new TwoCommandOrchestrator(SimpleMessage(id = 5L))),
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

    parentProxy.expectMsg(15.seconds, ActionFinished(id = 5L))
  }
}
