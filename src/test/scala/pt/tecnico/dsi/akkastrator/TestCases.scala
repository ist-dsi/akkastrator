package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.{ActorPath, Props}
import akka.event.LoggingReceive
import akka.testkit.{TestDuration, TestProbe}
import pt.tecnico.dsi.akkastrator.Orchestrator.{CorrelationId, StartReadyTasks}
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.TestCases._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

class TestException(msg: String) extends Exception(msg) with NoStackTrace

object TestCases {
  def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task] = Set.empty[Task])
              (implicit orchestrator: Orchestrator): Task = {
    new Task(description, dependencies) {
      val destination: ActorPath = _destination
      def createMessage(deliveryId: CorrelationId): Any = SimpleMessage(deliveryId)

      def behavior: Receive = {
        case m @ SimpleMessage(id) if matchSenderAndId(id) =>
          finish(m, id)
      }
    }
  }

  case object Finish

  abstract class ControllableOrchestrator extends Orchestrator {
    override def persistenceId: String = this.getClass.getSimpleName

    //No snapshots
    override def saveSnapshotEveryXMessages: Int = 0

    override def onFinish(): Unit = {
      context.become(orchestratorReceiveCommand orElse LoggingReceive {
        case Finish ⇒
          log.info("Finish!")
          context stop self
      })
    }

    //Prevent the orchestrator from starting the tasks immediately
    override def onRecoveryComplete(): Unit = ()

    //Add a case to always be able to crash the orchestrator
    override def orchestratorReceiveCommand: Receive = super.orchestratorReceiveCommand orElse {
      case "boom" ⇒ throw new TestException("BOOM")
    }
  }

  class TestCase1Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    // A
    echoTask("A", dests(0).ref.path)
  }

  class TestCase2Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    // A
    // B
    echoTask("A", dests(0).ref.path)
    echoTask("B", dests(1).ref.path)
  }

  class TestCase3Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    // A → B
    val a = echoTask("A", dests(0).ref.path)
    echoTask("B", dests(1).ref.path, dependencies = Set(a))
  }

  class TestCase4Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    // A
    //  ⟩→ C
    // B
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path)
    echoTask("C", dests(2).ref.path, dependencies = Set(a, b))
  }

  class TestCase5Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    //   B
    //  ↗ ↘
    // A → C
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path, dependencies = Set(a))
    echoTask("C", dests(2).ref.path, dependencies = Set(a, b))
  }

  class TestCase6Orchestrator(dests: Array[TestProbe]) extends ControllableOrchestrator {
    // A → B
    //   ↘  ⟩→ C
    // C → D
    val a = echoTask("A", dests(0).ref.path)
    val b = echoTask("B", dests(1).ref.path, dependencies = Set(a))
    val c = echoTask("C", dests(2).ref.path)
    val d = echoTask("D", dests(3).ref.path, dependencies = Set(a, c))
    echoTask("E", dests(4).ref.path, dependencies = Set(b, d))
  }
}

trait TestCases { self: ActorSysSpec ⇒

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
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int) {
    val destinations = Array.fill(numberOfDestinations)(TestProbe())

    lazy val orchestratorActor = system.actorOf(Props(implicitly[ClassTag[O]].runtimeClass, destinations))

    val firstState: State = {
      val statuses = Seq.tabulate(numberOfDestinations)(i ⇒ (i, Set(Unstarted): Set[Task.Status]))
      new State(SortedMap(statuses:_*))
    }

    val transformations: Seq[State ⇒ State]

    private val lastTransformation: State ⇒ State = { s ⇒
      orchestratorActor ! Finish
      s
    }

    def testForEachState(test: (TestProbe, State) ⇒ Unit): Unit = {
      val probe = TestProbe()

      (transformations :+ lastTransformation).foldLeft(firstState) {
        case (lastState, transformationFunction) ⇒
          //Perform the tests for lastState
          test(probe, lastState)

          //logger.info("Going to next state")
          transformationFunction(lastState)
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
  val testCase1 = new TestCase[TestCase1Orchestrator](1) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting
        )
      }, { secondState ⇒
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
    *    · After both tasks finish
    */
  val testCase2 = new TestCase[TestCase2Orchestrator](2) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting,
          1 -> Waiting
        )
      }, { secondState ⇒
        destinationExpectMsgAndReply(0)
        destinationExpectMsgAndReply(1)

        secondState.updatedExactStatuses(
          0 -> Finished,
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
  val testCase3 = new TestCase[TestCase3Orchestrator](2) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting
        )
      }, { secondState ⇒
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
  val testCase4 = new TestCase[TestCase4Orchestrator](3) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting,
          1 -> Waiting
        )
      }, { secondState ⇒
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
  val testCase5 = new TestCase[TestCase5Orchestrator](3) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting
        )
      }, { secondState ⇒
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
  val testCase6 = new TestCase[TestCase6Orchestrator](5) {
    val transformations: Seq[State ⇒ State] = Seq(
      { firstState ⇒
        orchestratorActor ! StartReadyTasks

        firstState.updatedExactStatuses(
          0 -> Waiting,
          2 -> Waiting
        )
      }, { secondState ⇒
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
