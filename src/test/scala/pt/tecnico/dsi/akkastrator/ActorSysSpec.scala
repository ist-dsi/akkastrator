package pt.tecnico.dsi.akkastrator

import java.io.File

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace
import akka.actor.{Status => _, _}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL.{FullTask, TaskBuilder}
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.{Unstarted, Waiting}
import shapeless.ops.hlist.Tupler
import shapeless.{HList, HNil}

object ActorSysSpec {
  case object FinishOrchestrator
  case object OrchestratorFinished
  case object OrchestratorAborted

  val testsAbortReason = new Exception() with NoStackTrace
  
  abstract class ControllableOrchestrator(destinations: Array[TestProbe], startAndTerminateImmediately: Boolean = false)
    extends DistinctIdsOrchestrator {
    def task[R](destinationIndex: Int, result: R = "finished", abortOnReceive: Boolean = false): TaskBuilder[R] = {
      new Task[R](_) {
        val destination: ActorPath = destinations(destinationIndex).ref.path
        def createMessage(id: Long): Serializable = SimpleMessage(id)
        def behavior: Receive =  {
          case SimpleMessage(id) if matchId(id) =>
            if (abortOnReceive) {
              abort(testsAbortReason)
            } else {
              finish(result)
            }
        }
      }
    }
    
    def simpleMessageFulltask[R, DL <: HList, RL <: HList, RP](description: String, destinationIndex: Int,
                                                               result: R = "finished", dependencies: DL = HNil: HNil,
                                                               timeout: Duration = Duration.Inf, abortOnReceive: Boolean = false)
                                                              (implicit orchestrator: AbstractOrchestrator[_],
                                                               tc: TaskComapped.Aux[DL, RL] = TaskComapped.nil,
                                                               tupler: Tupler.Aux[RL, RP] = Tupler.hnilTupler): FullTask[R, DL] = {
      FullTask(description, dependencies, timeout) createTaskWith { _ =>
        task(destinationIndex, result, abortOnReceive)
      }
    }
  
    override def persistenceId: String = this.getClass.getSimpleName
    
    if (startAndTerminateImmediately) {
      self ! StartOrchestrator(1L)
    }
  
    override def onFinish(): Unit = {
      context.parent ! OrchestratorFinished
      terminate(super.onFinish())
    }
  
    override def onAbort(failure: Failure): Unit = {
      context.parent ! OrchestratorAborted
      terminate(super.onAbort(failure))
    }
    
    def terminate(terminate: => Unit): Unit = {
      if (startAndTerminateImmediately) {
        terminate
      } else {
        // Having an extra last step to terminate the orchestrator allows us to test if the orchestrator
        // recovers to a good state if it crashes just before terminating.
        context become (computeCurrentBehavior() orElse {
          case FinishOrchestrator => terminate
        })
      }
    }
  
    override def extraCommands: Receive = {
      case "boom" =>
        throw new IllegalArgumentException("BOOM") with NoStackTrace
    }
  }
  
  // Quite ugly but makes the tests more readable
  implicit def state2SetOfState[S <: Task.State](s: S): Set[Task.State] = Set(s)
  implicit class RichTaskState(val tuple: (Int, Task.State)) extends AnyVal {
    def or(other: Task.State): (Int, Set[Task.State]) = {
      tuple match {
        case (taskIndex, firstState) => (taskIndex, Set(firstState, other))
      }
    }
  }
}

abstract class ActorSysSpec(config: Option[Config] = None) extends TestKit(ActorSystem("ActorSysSpec", config = config))
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with LazyLogging {
  
  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir"
  ).map(s => new File(system.settings.config.getString(s)))
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
    storageLocations.foreach(FileUtils.deleteDirectory)
    shutdown(verifySystemShutdown = true)
  }
  
  case class State(expectedStatus: Map[Int, Set[Task.State]]) {
    def updatedStatuses(newStatuses: (Int, Set[Task.State])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskIndex, possibleStatus)) =>
          statuses.updated(taskIndex, possibleStatus)
      }
      this.copy(expectedStatus = newExpectedStatus)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasksIndexes: Set[Int]) {
    private val probe = TestProbe()

    val parentProbe = TestProbe("parent-probe")
    val destinations = Array.fill(numberOfDestinations)(TestProbe("dest"))

    val orchestratorActor = system.actorOf(Props(new Actor {
      val orchestratorClass = implicitly[ClassTag[O]].runtimeClass
      val child = context.actorOf(Props(orchestratorClass, destinations), orchestratorClass.getSimpleName)

      context watch child

      def receive: Actor.Receive = {
        case Terminated(`child`) => context stop self
        case m if sender == child => parentProbe.ref forward m
        case m => child forward m
      }
    }))

    probe watch orchestratorActor

    val firstState: State = State(SortedMap.empty).updatedStatuses (
      startingTasksIndexes.toSeq.map(startingTask => (startingTask, Set[Task.State](Unstarted))):_*
    )
    
    val startTransformation: State => State = { firstState =>
      logger.info(s"Starting the Orchestrator")
      orchestratorActor ! StartOrchestrator(1L)
      firstState.copy(expectedStatus = firstState.expectedStatus.mapValues(_ => Waiting))
    }
    val finishTransformation: State => State = { s =>
      logger.info("Sending FinishOrchestrator")
      orchestratorActor ! FinishOrchestrator
      s
    }
    val transformations: Seq[State => State]
    def withStartAndFinishTransformations(transformations: (State => State)*): Seq[State => State] = {
      startTransformation +: transformations :+ finishTransformation
    }
    
    def pingPong(destination: TestProbe, ignoreTimeoutError: Boolean = false, pong: Boolean = true): Unit = {
      try {
        val m = destination.expectMsgType[SimpleMessage]
        logger.info(s"$m: ${destination.sender()} ${if (pong) "<" else ""}-> ${destination.ref}")
        if (pong) destination reply m
      } catch {
        case e: AssertionError if e.getMessage.contains("timeout") && ignoreTimeoutError =>
          // Purposefully ignored
      }
    }

    def sameTestPerState(test: State => Unit): Unit = {
      val sameTestRepeated = Seq.fill(transformations.size)(test)
      differentTestPerState(sameTestRepeated:_*)
    }
    def differentTestPerState(tests: (State => Unit)*): Unit = {
      var i = 1
      val testsAndTransformations: Seq[(State => Unit, State => State)] = tests zip transformations
      testsAndTransformations.foldLeft(firstState) { case (lastState, (test, transformation)) =>
        val expectedStateString = lastState.expectedStatus.mapValues(_.mkString(" or ")).mkString("\n\t", "\n\t", "\n\t")
        logger.info(s"""=== STATE $i =============================
                       |EXPECTING:$expectedStateString""".stripMargin)
        test(lastState)
        i += 1

        logger.info(s"=== Apply transformation to get to State $i ===========================")

        val newState = transformation(lastState)
        logger.info("\n\n\n\n")
        newState
      }
    }
    
    def testStatus(state: State, max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, probe.ref)
      val taskState = probe.expectMsgType[StatusResponse](max)
        .tasks
        .map(t => (t.index, t.state))
        .toMap
      for((index, expected) <- state.expectedStatus) {
        // expected should contain (taskState(description))
        // does not work for the Aborted state because Exception does not implement its own equals.
        expected contains taskState(index)
      }
      logger.info("Status matched!")
    }
    
    def testExpectedStatusWithRecovery(): Unit = {
      sameTestPerState { state =>
        // Test if the orchestrator is in the expected state (aka the status is what we expect)
        testStatus(state)
        // Crash the orchestrator
        orchestratorActor ! "boom"
        // Test that the orchestrator recovered to the expected state
        testStatus(state)
      }

      probe expectTerminated orchestratorActor
    }
    
    def expectInnerOrchestratorTermination(taskIndex: Int): Unit = {
      orchestratorActor.tell(Status, probe.ref)
      probe.expectMsgType[StatusResponse].tasks.foreach {
        case Report(`taskIndex`, _, _, Task.Unstarted, _, _) =>
          throw new IllegalStateException("Cannot expect for inner orchestrator termination if the inner orchestrator hasn't started.")
        case Report(`taskIndex`, _, _, Task.Waiting, Some(destination), _) =>
          // To be able to watch an actor we need its ActorRef first
          system.actorSelection(destination).tell(Identify(1L), probe.ref)
          probe.expectMsgType[ActorIdentity] match {
            case ActorIdentity(1L, Some(ref)) =>
              // We know that when an orchestrator finishes or aborts it stops it self.
              // So we just need to expect for its termination.
              logger.info(s"Task $taskIndex destination ActorRef $ref")
              probe watch ref
              probe expectTerminated ref
            case _ =>
              // The innerOrchestrator already finished, so we don't need to do anything
          }
        case _ =>
          // The task already finished or aborted
      }
    }
  }
}
