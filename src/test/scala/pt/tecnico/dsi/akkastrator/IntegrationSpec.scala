package pt.tecnico.dsi.akkastrator

import akka.actor.Actor._
import akka.actor.{ActorPath, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.persistence.SnapshotSelectionCriteria
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

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

  def echoTask(name: String, _destination: ActorPath, dependencies: Set[Task] = Set.empty[Task])
              (implicit orchestrator: Orchestrator): Task = {
    import orchestrator.context //Needed for the LoggingReceive

    new Task(name, dependencies) {
      val destination: ActorPath = _destination
      def createMessage(deliveryId: Long): Any = SimpleMessage(deliveryId)

      def behavior: Receive = LoggingReceive {
        case m @ SimpleMessage(id) if matchDeliveryId(id) =>
          finish(m, id)
      }
    }
  }

  def NChainedTasksOrchestrator(numberOfTasks: Int): (Array[TestProbe], ActorRef) = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val destinations = Array.fill(numberOfTasks)(TestProbe())

    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
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

    val probe = TestProbe()
    probe.watch(orchestrator)

    for (i <- 0 until numberOfTasks) {
      val message = destinations(i).expectMsgClass(200.millis, classOf[SimpleMessage])
      for (j <- (i + 1) until numberOfTasks) {
        destinations(j).expectNoMsg(100.millis)
      }
      destinations(i).reply(SimpleMessage(message.id))
    }

    probe.expectMsgPF((numberOfTasks * 300).millis){ case Terminated(o) if o == orchestrator => true }
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

  def testStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: Duration = 300.millis)
                   (obtainedTasksMatch: Seq[TaskStatus] => Boolean): Unit = {
    val statusId = nextSeq()
    orchestrator.tell(Status(statusId), probe.ref)
    probe.expectMsgPF(maxDuration) {
      case StatusResponse(obtainedTasks, `statusId`) if obtainedTasksMatch(obtainedTasks) => true
    }
  }
}
