package pt.ulisboa.tecnico.dsi.akkastrator

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import akka.actor.Actor.Receive
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.event.LoggingReceive
import akka.persistence.Recovery
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import pt.ulisboa.tecnico.dsi.akkastrator.Message.{Message, MessageId}

case class SimpleMessage(id: MessageId)

class OrchestratorSpec extends TestKit(ActorSystem("Orchestrator", ConfigFactory.load()))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }


  def echoCommand(name: String, _destination: ActorPath, message: MessageId => SimpleMessage,
                  dependencies: Set[Command] = Set.empty[Command])(implicit orchestrator: Orchestrator): Command = {
    import orchestrator.context
    new Command(name, dependencies) {
      val destination: ActorPath = _destination
      def createMessage(id: MessageId): Message = message(id)

      def behavior: Receive = LoggingReceive {
        case m: SimpleMessage if matchSenderAndId(m) =>
          orchestrator.log.info(s"$loggingPrefix Received $m from ${orchestrator.sender()}")
          finish(m)
      }
    }
  }

  def fabricatedParent(childProps: Props, childName: String): (TestProbe, ActorRef) = {
    val parentProxy = TestProbe()
    val parent = system.actorOf(Props(new Actor {

      override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1.minute) {
        case _: Throwable => Stop
      }

      val child = context.actorOf(childProps, childName)
      def receive = {
        case x if sender == child => parentProxy.ref forward x
        case x => child forward x
      }
    }))

    (parentProxy, parent)
  }

  "Orchestrator" should {
    "Send one message, handle the response and finish" in {
      val destinationActor0 = TestProbe()

      class OneCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
        //No state, snapshots and recovery
        var state = Orchestrator.EmptyState
        val saveSnapshotInterval: FiniteDuration = Duration.Zero
        override def preStart() = self ! Recovery(toSequenceNr = 0L)

        echoCommand("Zero Command", destinationActor0.ref.path, id => SimpleMessage(id))
      }

      val (parentProxy, _) = fabricatedParent(
        Props(new OneCommandOrchestrator(SimpleMessage(1L))),
        "OneSimpleCommandOrchestrator"
      )

      val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
      destinationActor0.reply(SimpleMessage(a0m.id))

      parentProxy.expectMsg(5.seconds, ActionFinished(1L))
    }
    "Send two messages, handle the response with the same type and finish" in {
      val destinationActor0 = TestProbe()
      val destinationActor1 = TestProbe()

      class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
        //No state, snapshots and recovery
        var state = Orchestrator.EmptyState
        val saveSnapshotInterval: FiniteDuration = Duration.Zero
        override def preStart() = self ! Recovery(toSequenceNr = 0L)

        echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
        echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
      }

      val (parentProxy, _) = fabricatedParent(
        Props(new TwoCommandOrchestrator(SimpleMessage(2L))),
        "TwoSimpleCommandOrchestrator"
      )

      val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
      val a1m = destinationActor1.expectMsgClass(2.seconds, classOf[SimpleMessage])
      destinationActor0.reply(SimpleMessage(a0m.id))
      destinationActor1.reply(SimpleMessage(a1m.id))

      parentProxy.expectMsg(5.seconds, ActionFinished(2L))
    }

    "Handle dependencies: Zero -> One" in {
      val destinationActor0 = TestProbe()
      val destinationActor1 = TestProbe()

      class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
        //No state, snapshots and recovery
        var state = Orchestrator.EmptyState
        val saveSnapshotInterval: FiniteDuration = Duration.Zero
        override def preStart() = self ! Recovery(toSequenceNr = 0L)

        val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
        echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
      }

      val (parentProxy, _) = fabricatedParent(
        Props(new TwoCommandOrchestrator(SimpleMessage(3L))),
        "TwoDependentCommandOrchestrator"
      )

      val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
      destinationActor1.expectNoMsg(2.seconds)
      destinationActor0.reply(SimpleMessage(a0m.id))

      val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
      destinationActor1.reply(SimpleMessage(a1m.id))

      parentProxy.expectMsg(15.seconds, ActionFinished(3L))
    }
    "Handle dependencies: Zero -> One -> Two" in {
      val destinationActor0 = TestProbe()
      val destinationActor1 = TestProbe()
      val destinationActor2 = TestProbe()

      class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
        //No state, snapshots and recovery
        var state = Orchestrator.EmptyState
        val saveSnapshotInterval: FiniteDuration = Duration.Zero
        override def preStart() = self ! Recovery(toSequenceNr = 0L)

        val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
        val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage, Set(zeroCommand))
        echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(oneCommand))
      }

      val (parentProxy, _) = fabricatedParent(
        Props(new TwoCommandOrchestrator(SimpleMessage(4L))),
        "ThreeDependentCommandOrchestrator"
      )

      val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
      destinationActor1.expectNoMsg(2.seconds)
      destinationActor2.expectNoMsg(2.seconds)
      destinationActor0.reply(SimpleMessage(a0m.id))

      val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
      destinationActor2.expectNoMsg(2.seconds)
      destinationActor1.reply(SimpleMessage(a1m.id))

      val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
      destinationActor2.reply(SimpleMessage(a2m.id))

      parentProxy.expectMsg(15.seconds, ActionFinished(4L))
    }
    "Handle dependencies: (Zero, One) -> Two" in {
      val destinationActor0 = TestProbe()
      val destinationActor1 = TestProbe()
      val destinationActor2 = TestProbe()

      class TwoCommandOrchestrator(m: SimpleMessage) extends Orchestrator(m) {
        //No state, snapshots and recovery
        var state = Orchestrator.EmptyState
        val saveSnapshotInterval: FiniteDuration = Duration.Zero
        override def preStart() = self ! Recovery(toSequenceNr = 0L)

        val zeroCommand = echoCommand("Zero Command", destinationActor0.ref.path, SimpleMessage)
        val oneCommand = echoCommand("One Command", destinationActor1.ref.path, SimpleMessage)
        echoCommand("Two Command", destinationActor2.ref.path, SimpleMessage, Set(zeroCommand, oneCommand))
      }

      val (parentProxy, _) = fabricatedParent(
        Props(new TwoCommandOrchestrator(SimpleMessage(5L))),
        "ThreeDependentCommandOrchestrator"
      )

      val a0m = destinationActor0.expectMsgClass(2.seconds, classOf[SimpleMessage])
      destinationActor2.expectNoMsg(2.seconds)
      destinationActor0.reply(SimpleMessage(a0m.id))

      val a1m = destinationActor1.expectMsgClass(10.seconds, classOf[SimpleMessage])
      destinationActor2.expectNoMsg(2.seconds)
      destinationActor1.reply(SimpleMessage(a1m.id))

      val a2m = destinationActor2.expectMsgClass(10.seconds, classOf[SimpleMessage])
      destinationActor2.reply(SimpleMessage(a2m.id))

      parentProxy.expectMsg(15.seconds, ActionFinished(5L))
    }

    //Handle Retry
    //Handle Status
    //Re sends
    //Persistence
    //Snapshots
  }
}
