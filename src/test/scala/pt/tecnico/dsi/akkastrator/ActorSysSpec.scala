package pt.tecnico.dsi.akkastrator

import java.io.File

import akka.actor.{ActorPath, ActorRef, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.testkit.{TestDuration, TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.IdImplicits._
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.TestCaseOrchestrators._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

class TestException(msg: String) extends Exception(msg) with NoStackTrace

object TestCaseOrchestrators {
  case object Finish
  case object TerminatedEarly

  abstract class ControllableOrchestrator(terminationProbe: ActorRef,
                                          startAndTerminateImmediately: Boolean = false) extends DistinctIdsOrchestrator {
    def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task] = Set.empty[Task],
                 earlyTermination: Boolean = false): Task = {
      new Task(description, dependencies) {
        val destination: ActorPath = _destination
        def createMessage(id: CorrelationId): Any = SimpleMessage(description, id.self)

        def behavior: Receive = /*LoggingReceive.withLabel(f"Task [$index%02d - $description]")*/ {
          case m @ SimpleMessage(_, id) if matchSenderAndId(id) =>
            if (earlyTermination) {
              terminateEarly(m, new CorrelationId(id))
            } else {
              finish(m, id)
            }
        }
      }
    }

    override def persistenceId: String = this.getClass.getSimpleName

    //No automatic snapshots
    override def saveSnapshotRoughlyEveryXMessages: Int = 0

    override def startTasks(): Unit = {
      if (startAndTerminateImmediately) {
        super.startTasks()
      }
    }

    override def onFinish(): Unit = {
      if (startAndTerminateImmediately) {
        super.onFinish()
      } else {
        //Prevent the orchestrator from stopping as soon as all the tasks finish
        //We still want to handle Status messages
        context.become(orchestratorCommand orElse LoggingReceive {
          case Finish ⇒ super.onFinish()
        })
      }
    }

    override def onEarlyTermination(instigator: Task, message: Any, tasks: Map[pt.tecnico.dsi.akkastrator.Task.State, Seq[Task]]): Unit = {
      log.info("Terminated Early")
      terminationProbe ! TerminatedEarly
    }

    //Add a case to always be able to crash the orchestrator
    override def extraCommands: Receive = {
      case "boom" ⇒ throw new TestException("BOOM")
    }
  }

  class SingleTaskOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    echoTask("A", destinations(0).ref.path)
  }
  class TwoIndependentTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    // B
    echoTask("A", destinations(0).ref.path)
    echoTask("B", destinations(0).ref.path)
  }
  class TwoLinearTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    val a = echoTask("A", destinations(0).ref.path)
    echoTask("B", destinations(0).ref.path, dependencies = Set(a))
  }
  class TasksInTOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    //  ⟩→ C
    // B
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(1).ref.path)
    echoTask("C", destinations(2).ref.path, dependencies = Set(a, b))
  }
  class TasksInTriangleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //   B
    //  ↗ ↘
    // A → C
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(0).ref.path, dependencies = Set(a))
    echoTask("C", destinations(1).ref.path, dependencies = Set(a, b))
  }
  class FiveTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    //   ↘  ⟩→ C
    // C → D
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(1).ref.path, dependencies = Set(a))
    val c = echoTask("C", destinations(2).ref.path)
    val d = echoTask("D", destinations(3).ref.path, dependencies = Set(a, c))
    echoTask("E", destinations(4).ref.path, dependencies = Set(b, d))
  }

  class EarlyStopSingleTaskOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    echoTask("A", destinations(0).ref.path, earlyTermination = true)
  }
  class EarlyStopTwoIndependentTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    // B
    echoTask("A", destinations(0).ref.path, earlyTermination = true)
    echoTask("B", destinations(1).ref.path)
  }
  class EarlyStopTwoLinearTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    val a = echoTask("A", destinations(0).ref.path, earlyTermination = true)
    echoTask("B", destinations(1).ref.path, dependencies = Set(a))
  }
}

abstract class ActorSysSpec extends TestKit(ActorSystem("Orchestrator"))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with LazyLogging {

  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir"
  ).map(s ⇒ new File(system.settings.config.getString(s)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    storageLocations.foreach(FileUtils.deleteDirectory)
    shutdown(verifySystemShutdown = true)
  }

  def testStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: FiniteDuration = 500.millis.dilated)
                   (expectedStatus: SortedMap[Int, Set[Task.State]]): Unit = {
    orchestrator.tell(Status, probe.ref)
    val obtainedStatus: IndexedSeq[Task.State] = probe.expectMsgClass(maxDuration, classOf[StatusResponse]).tasks.map(_.status).toIndexedSeq

    for((index, expected) <- expectedStatus) {
      expected should contain (obtainedStatus(index))
    }
  }
  def testExactStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: FiniteDuration = 500.millis.dilated)
                        (expectedStatus: Task.State*): Unit = {
    orchestrator.tell(Status, probe.ref)

    val obtainedStatus = probe.expectMsgClass(maxDuration, classOf[StatusResponse]).tasks.map(_.status)
    obtainedStatus.zip(expectedStatus).foreach { case (obtained, expected) ⇒
      obtained shouldEqual expected
    }
  }

  case class State(expectedStatus: SortedMap[Int, Set[Task.State]]) {
    def updatedStatuses(newStatuses: (Symbol, Set[Task.State])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskSymbol, possibleStatus)) ⇒
          statuses.updated(taskSymbol.name.head - 'A', possibleStatus)
      }
      this.copy(newExpectedStatus)
    }
    def updatedExactStatuses(newStatuses: (Symbol, Task.State)*): State = {
      updatedStatuses(newStatuses.map { case (taskSymbol, status) ⇒
        (taskSymbol, Set(status))
      }:_*)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasks: Set[Symbol]) {
    val terminationProbe = TestProbe("termination-probe")
    val statusProbe = TestProbe("status-probe")
    val destinations = Array.tabulate(numberOfDestinations)(i ⇒ TestProbe(s"Dest-$i"))

    private val orchestratorClass = implicitly[ClassTag[O]].runtimeClass
    lazy val orchestratorActor = system.actorOf(
      Props(orchestratorClass, destinations, terminationProbe.ref),
      orchestratorClass.getSimpleName
    )

    terminationProbe.watch(orchestratorActor)

    val firstState: State = {
      State(SortedMap.empty).updatedExactStatuses(startingTasks.toSeq.map(i ⇒ (i, Unstarted)):_*)
    }

    val firstTransformation: State ⇒ State = { s ⇒
      orchestratorActor ! StartReadyTasks

      firstState.updatedExactStatuses(startingTasks.toSeq.map(i ⇒ (i, Waiting)):_*)
    }
    val transformations: Seq[State ⇒ State]
    val lastTransformation: State ⇒ State = { s ⇒
      orchestratorActor ! Finish
      s
    }

    private lazy val allTransformations = firstTransformation +: transformations :+ lastTransformation

    //Performs the same test in each state
    def sameTestPerState(test: State ⇒ Unit): Unit = {
      var i = 1
      allTransformations.foldLeft(firstState) {
        case (lastState, transformationFunction) ⇒
          logger.info(s"STATE #$i expecting\n$lastState\n")
          test(lastState)
          i += 1
          logger.info(s"Computing state #$i")
          val newState = transformationFunction(lastState)
          logger.info(s"Computed state #$i\n\n\n")
          newState
      }
    }

    //Performs a different test in each state
    def differentTestPerState(tests: (State ⇒ Unit)*): Unit = {
      val testsAndTransformations: Seq[(State ⇒ Unit, State ⇒ State)] = tests.zip(allTransformations)
      testsAndTransformations.foldLeft(firstState) {
        case (lastState, (test, transformation)) ⇒
          //Perform the test for lastState
          test(lastState)

          transformation(lastState)
      }
    }

    def pingPongDestinationNumber(number: Int): Unit = {
      val destination = destinations(number)
      val m = destination.expectMsgClass(classOf[SimpleMessage])
      logger.info(s"${destination.ref.path.name} received message $m")
      destination.reply(m)
    }
  }

  /** Graph:
    *  A
    *
    *  Test points:
    *    · Before any task starts
    *    · After task A starts
    *    · After Task A finishes
    */
  lazy val testCase1 = new TestCase[SingleTaskOrchestrator](1, Set('A)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(0)

        secondState.updatedExactStatuses(
          'A -> Finished
        )
      }
    )
  }

  /** Graph:
    *  A
    *  B
    *
    *  Test points:
    *    · Before any task starts
    *    · After both tasks start
    *    · After task A finishes
    *    · After task B finishes
    */
  lazy val testCase2 = new TestCase[TwoIndependentTasksOrchestrator](1, Set('A, 'B)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(0)

        secondState.updatedExactStatuses(
          'A -> Finished
        )
      }, { thirdState ⇒
        pingPongDestinationNumber(0)

        thirdState.updatedExactStatuses(
          'B -> Finished
        )
      }
    )
  }

  /** Graph:
    *  A → B
    *
    *  Test points:
    *   · Before any task starts
    *   · After Task A starts
    *   · After Task A finishes and Task B is about to start or has already started
    *   · After Task B finishes
    */
  lazy val testCase3 = new TestCase[TwoLinearTasksOrchestrator](1, Set('A)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(0)

        secondState.updatedExactStatuses(
          'A -> Finished
        ).updatedStatuses(
          'B -> Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        //TODO: WTF task A is resending the message
        //pingPongDestinationNumber(0)
        pingPongDestinationNumber(0)

        thirdState.updatedExactStatuses(
          'B -> Finished
        )
      }
    )
  }

  /** Graph:
    *  A
    *   ⟩→ C
    *  B
    *
    *  Test points:
    *   · Before any task starts
    *   · After Task A and Task B start
    *   · After Task B finishes
    *   · After Task B finishes
    */
  lazy val testCase4 = new TestCase[TasksInTOrchestrator](3, Set('A, 'B)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(1)

        secondState.updatedExactStatuses(
          'B -> Finished
        )
      }, { thirdState ⇒
        pingPongDestinationNumber(0)

        thirdState.updatedExactStatuses(
          'A -> Finished
        ).updatedStatuses(
          'C -> Set(Unstarted, Waiting)
        )
      }, { fourthState ⇒
        pingPongDestinationNumber(2)

        fourthState.updatedExactStatuses(
          'C -> Finished
        )
      }
    )
  }

  /** Graph:
    *    B
    *   ↗ ↘
    *  A → C
    *
    *  Test points:
    *   · Before any task starts
    *   · After Task A starts
    *   · After Task A finishes and Task B is about to start or has already started
    *   · After Task B finishes and Task C is about to start or has already started
    *   · After Task C finishes
    */
  lazy val testCase5 = new TestCase[TasksInTriangleOrchestrator](2, Set('A)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(0)
        
        secondState.updatedExactStatuses(
          'A → Finished
        ).updatedStatuses(
          'B → Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        //TODO: WTF task A is resending the message
        pingPongDestinationNumber(0)
        pingPongDestinationNumber(0)

        thirdState.updatedExactStatuses(
          'B → Finished
        ).updatedStatuses(
          'C → Set(Unstarted, Waiting)
        )
      }, { fourthState ⇒
        pingPongDestinationNumber(1)

        fourthState.updatedExactStatuses(
          'C → Finished
        )
      }
    )
  }

  /** Graph:
    *  A → B
    *    ↘  ⟩→ E
    *  C → D
    *
    *  Test points:
    *   · Before any task starts
    *   · After Task A and C start
    *   · After Task A finishes and Task B is about to start or has already started
    *   · After Task B finishes
    *   · After Task C finishes and Task D is about to start or has already started
    *   · After Task D finishes and Task E is about to start or has already started
    *   · After Task E finishes
    */
  lazy val testCase6 = new TestCase[FiveTasksOrchestrator](5, Set('A, 'C)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        pingPongDestinationNumber(0)

        secondState.updatedExactStatuses(
          'A -> Finished
        ).updatedStatuses(
          'B -> Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        pingPongDestinationNumber(1)

        thirdState.updatedExactStatuses(
          'B -> Finished
        )
      }, { fourthState ⇒
        pingPongDestinationNumber(2)

        fourthState.updatedExactStatuses(
          'C -> Finished
        ).updatedStatuses(
          'D -> Set(Unstarted, Waiting)
        )
      }, { fifthState ⇒
        pingPongDestinationNumber(3)

        fifthState.updatedExactStatuses(
          'D -> Finished
        ).updatedStatuses(
          'E -> Set(Unstarted, Waiting)
        )
      }, { sixthState ⇒
        pingPongDestinationNumber(4)

        sixthState.updatedExactStatuses(
          'E -> Finished
        )
      }
    )
  }
}
