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

  abstract class ControllableOrchestrator(probe: ActorRef, startImmediately: Boolean = true,
                                          terminateImmediately: Boolean = false) extends DistinctIdsOrchestrator {
    def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task] = Set.empty[Task],
                 earlyTermination: Boolean = false): Task = {
      new Task(description, dependencies) {
        val destination: ActorPath = _destination
        def createMessage(id: CorrelationId): Any = SimpleMessage(id.self)

        def behavior: Receive = {
          case m @ SimpleMessage(id) if matchSenderAndId(id) =>
            if (earlyTermination) {
              terminateEarly(m, id)
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
      if (startImmediately) {
        super.startTasks()
      }
    }

    override def onFinish(): Unit = {
      if (terminateImmediately) {
        super.onFinish()
      } else {
        //Prevent the orchestrator from stopping as soon as all the tasks finish
        //We still want to handle Status messages
        context.become(orchestratorCommand orElse LoggingReceive {
          case Finish ⇒ super.onFinish()
        })
      }
    }

    override def onEarlyTermination(instigator: Task, message: Any, tasks: Map[pt.tecnico.dsi.akkastrator.Task.Status, Seq[Task]]): Unit = {
      log.info("Terminated Early")
      probe ! TerminatedEarly
    }

    //Add a case to always be able to crash the orchestrator
    override def extraCommands: Receive = {
      case "boom" ⇒ throw new TestException("BOOM")
    }
  }

  class SingleTaskOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    echoTask("A", dests(0).ref.path)
  }
  class TwoIndependentTasksOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    // B
    echoTask("A", dests(0).ref.path)
    echoTask("B", dests(1).ref.path)
  }
  class TwoLinearTasksOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    val a = echoTask("A", dests(0).ref.path)
    echoTask("B", dests(1).ref.path, dependencies = Set(a))
  }
  class TasksInTOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    //  ⟩→ C
    // B
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path)
    echoTask("C", dests(2).ref.path, dependencies = Set(a, b))
  }
  class TasksInTriangleOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //   B
    //  ↗ ↘
    // A → C
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path, dependencies = Set(a))
    echoTask("C", dests(2).ref.path, dependencies = Set(a, b))
  }
  class FiveTasksOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    //   ↘  ⟩→ C
    // C → D
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path, dependencies = Set(a))
    val c = echoTask("C", dests(2).ref.path)
    val d = echoTask("D", dests(3).ref.path, dependencies = Set(a, c))
    echoTask("E", dests(4).ref.path, dependencies = Set(b, d))
  }

  abstract class EarlyStopOrchestrator(probe: ActorRef) extends ControllableOrchestrator(probe, startImmediately = false)

  class EarlyStopSingleTaskOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends EarlyStopOrchestrator(probe) {
    // A
    echoTask("A", dests(0).ref.path, earlyTermination = true)
  }
  class EarlyStopTwoIndependentTasksOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends EarlyStopOrchestrator(probe) {
    // A
    // B
    echoTask("A", dests(0).ref.path, earlyTermination = true)
    echoTask("B", dests(1).ref.path)
  }
  class EarlyStopTwoLinearTasksOrchestrator(dests: Array[TestProbe], probe: ActorRef) extends EarlyStopOrchestrator(probe) {
    // A → B
    val a = echoTask("A", dests(0).ref.path, earlyTermination = true)
    echoTask("B", dests(1).ref.path, dependencies = Set(a))
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

  def NChainedTasksOrchestrator(numberOfTasks: Int): (Array[TestProbe], ActorRef) = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val destinations = Array.fill(numberOfTasks)(TestProbe())

    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new ControllableOrchestrator(TestProbe().ref, terminateImmediately = true) {
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
        destinations(i).reply(SimpleMessage(message.id))
      }
    }
  }

  def withOrchestratorTermination(orchestrator: ActorRef)(test: => Unit): Unit = {
    val probe = TestProbe()
    probe.watch(orchestrator)
    test
    probe.expectTerminated(orchestrator)
  }

  def testStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: FiniteDuration = 500.millis.dilated)
                   (expectedStatus: SortedMap[Int, Set[Task.Status]]): Unit = {
    orchestrator.tell(Status, probe.ref)
    val obtainedStatus: IndexedSeq[Task.Status] = probe.expectMsgClass(maxDuration, classOf[StatusResponse]).tasks.map(_.status).toIndexedSeq

    for((index, expected) <- expectedStatus) {
      expected should contain (obtainedStatus(index))
    }
  }
  def testExactStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: FiniteDuration = 500.millis.dilated)
                        (expectedStatus: Task.Status*): Unit = {
    orchestrator.tell(Status, probe.ref)

    val obtainedStatus = probe.expectMsgClass(maxDuration, classOf[StatusResponse]).tasks.map(_.status)
    obtainedStatus.zip(expectedStatus).foreach { case (obtained, expected) ⇒
      obtained shouldEqual expected
    }
  }

  case class State(expectedStatus: SortedMap[Int, Set[Task.Status]]) {
    def updatedStatuses(newStatuses: (Int, Set[Task.Status])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (index, possibleStatus)) ⇒
          statuses.updated(index, possibleStatus)
      }
      this.copy(newExpectedStatus)
    }
    def updatedExactStatuses(newStatuses: (Int, Task.Status)*): State = {
      updatedStatuses(newStatuses.map { case (index, status) ⇒
        (index, Set(status))
      }:_*)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasks: Set[Int]) {
    val terminationProbe = TestProbe("termination-probe")
    val statusProbe = TestProbe("status-probe")
    val destinations = Array.tabulate(numberOfDestinations)(i ⇒ TestProbe(s"destination-$i"))

    private val orchestratorClass = implicitly[ClassTag[O]].runtimeClass
    lazy val orchestratorActor = system.actorOf(
      Props(orchestratorClass, destinations, terminationProbe.ref),
      orchestratorClass.getSimpleName
    )

    terminationProbe.watch(orchestratorActor)

    val firstState: State = {
      val statuses = Seq.tabulate(numberOfDestinations) { i ⇒
        if (startingTasks.contains(i)) {
          //The tasks that start right await will have the state Unstarted, however
          //After the first crash their state will already be Waiting because they have already started
          (i, Set(Unstarted, Waiting): Set[Task.Status])
        } else {
          (i, Set(Unstarted): Set[Task.Status])
        }

      }
      new State(SortedMap(statuses:_*))
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
      allTransformations.foldLeft(firstState) {
        case (lastState, transformationFunction) ⇒
          //Perform the test for lastState
          test(lastState)

          transformationFunction(lastState)
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

    def destinationExpectMsgAndReply(destinationNumber: Int, maxDuration: FiniteDuration = 1.second.dilated): Unit = {
      val m = destinations(destinationNumber).expectMsgClass(maxDuration, classOf[SimpleMessage])
      destinations(destinationNumber).reply(SimpleMessage(m.id))
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
  lazy val testCase1 = new TestCase[SingleTaskOrchestrator](1, Set(0)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(0)

        secondState.updatedExactStatuses(
          0 -> Finished
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
  lazy val testCase2 = new TestCase[TwoIndependentTasksOrchestrator](2, Set(0, 1)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(0)

        secondState.updatedExactStatuses(
          0 -> Finished
        )
      }, { thirdState ⇒
        destinationExpectMsgAndReply(1)

        thirdState.updatedExactStatuses(
          1 -> Finished
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
  lazy val testCase3 = new TestCase[TwoLinearTasksOrchestrator](2, Set(0)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(0)

        secondState.updatedExactStatuses(
          0 -> Finished
        ).updatedStatuses(
          1 -> Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        destinationExpectMsgAndReply(1)

        thirdState.updatedExactStatuses(
          1 -> Finished
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
  lazy val testCase4 = new TestCase[TasksInTOrchestrator](3, Set(0, 1)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(1)

        secondState.updatedExactStatuses(
          1 -> Finished
        )
      }, { thirdState ⇒
        destinationExpectMsgAndReply(0)

        thirdState.updatedExactStatuses(
          0 -> Finished
        ).updatedStatuses(
          2 -> Set(Unstarted, Waiting)
        )
      }, { fourthState ⇒
        destinationExpectMsgAndReply(2)

        fourthState.updatedExactStatuses(
          2 -> Finished
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
  lazy val testCase5 = new TestCase[TasksInTriangleOrchestrator](3, Set(0)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(0)

        secondState.updatedExactStatuses(
          0 -> Finished
        ).updatedStatuses(
          1 -> Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        destinationExpectMsgAndReply(1)

        thirdState.updatedExactStatuses(
          1 -> Finished
        ).updatedStatuses(
          2 -> Set(Unstarted, Waiting)
        )
      }, { fourthState ⇒
        destinationExpectMsgAndReply(2)

        fourthState.updatedExactStatuses(
          2 -> Finished
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
  lazy val testCase6 = new TestCase[FiveTasksOrchestrator](5, Set(0, 2)) {
    val transformations: Seq[State ⇒ State] = Seq(
      { secondState ⇒
        destinationExpectMsgAndReply(0)

        secondState.updatedExactStatuses(
          0 -> Finished
        ).updatedStatuses(
          1 -> Set(Unstarted, Waiting)
        )
      }, { thirdState ⇒
        destinationExpectMsgAndReply(1)

        thirdState.updatedExactStatuses(
          1 -> Finished
        )
      }, { fourthState ⇒
        destinationExpectMsgAndReply(2)

        fourthState.updatedExactStatuses(
          2 -> Finished
        ).updatedStatuses(
          3 -> Set(Unstarted, Waiting)
        )
      }, { fifthState ⇒
        destinationExpectMsgAndReply(3)

        fifthState.updatedExactStatuses(
          3 -> Finished
        ).updatedStatuses(
          4 -> Set(Unstarted, Waiting)
        )
      }, { sixthState ⇒
        destinationExpectMsgAndReply(4)

        sixthState.updatedExactStatuses(
          4 -> Finished
        )
      }
    )
  }
}
