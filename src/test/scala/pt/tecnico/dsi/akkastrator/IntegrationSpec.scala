package pt.tecnico.dsi.akkastrator

import akka.actor.Actor._
import akka.actor.{ActorPath, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.persistence.SnapshotSelectionCriteria
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import pt.tecnico.dsi.akkastrator.Message.{Message, MessageId}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

abstract class IntegrationSpec extends TestKit(ActorSystem("Orchestrator", ConfigFactory.load()))
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  abstract class StatelessOrchestrator extends Orchestrator {
    //No state, snapshots and recovery
    var state = Orchestrator.EmptyState
    val saveSnapshotInterval: FiniteDuration = Duration.Zero

    override def postStop(): Unit = {
      super.postStop()
      deleteMessages(lastSequenceNr + 1)
      deleteSnapshots(SnapshotSelectionCriteria())
    }
  }

  def echoCommand(name: String, _destination: ActorPath, message: MessageId => SimpleMessage,
                  dependencies: Set[Command] = Set.empty[Command])(implicit orchestrator: Orchestrator): Command = {
    import orchestrator.context //Needed for the LoggingReceive

    new Command(name, dependencies) {
      val destination: ActorPath = _destination
      def createMessage(id: MessageId): Message = message(id)

      def behavior: Receive = LoggingReceive {
        case m: SimpleMessage if matchSenderAndId(m) =>
          finish(m)
      }
    }
  }

  def NChainedCommandsOrchestrator(n: Int): (Array[TestProbe], ActorRef) = {
    require(n >= 2, "Must have at least 2 commands")
    val destinations = Array.fill(n)(TestProbe())

    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      var last = echoCommand(letters(0).toString, destinations(0).ref.path, SimpleMessage)

      for (i <- 1 until n) {
        val current = echoCommand(letters(i).toString, destinations(i).ref.path, SimpleMessage, Set(last))
        last = current
      }
    }))

    (destinations, orchestrator)
  }
  def testNChainedEchoCommands(n: Int): Unit = {
    require(n >= 2, "Must have at least 2 commands")
    val (destinations, orchestrator) = NChainedCommandsOrchestrator(n)

    val probe = TestProbe()
    probe.watch(orchestrator)

    for (i <- 0 until n) {
      val message = destinations(i).expectMsgClass(100.millis, classOf[SimpleMessage])
      for (j <- (i + 1) until n) {
        destinations(j).expectNoMsg(50.millis)
      }
      destinations(i).reply(SimpleMessage(message.id))
    }

    probe.expectMsgPF((n * 200).millis){ case Terminated(o) if o == orchestrator => true }
  }

  def withOrchestratorTermination(orchestrator: ActorRef, maxDuration: Duration = 1.second)(f: TestProbe => Unit): Unit = {
    val probe = TestProbe()
    probe.watch(orchestrator)
    f(probe)
    probe.expectMsgPF(maxDuration){ case Terminated(o) if o == orchestrator => true }
  }

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def testStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: Duration = 200.millis)
                   (obtainedTasksMatch: Seq[Task] => Boolean): Unit = {
    val statusId = nextSeq()
    orchestrator.tell(Status(statusId), probe.ref)
    probe.expectMsgPF(maxDuration) {
      case StatusResponse(obtainedTasks, `statusId`) if obtainedTasksMatch(obtainedTasks) => true
    }
  }
}
