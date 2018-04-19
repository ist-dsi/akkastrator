package pt.tecnico.dsi.akkastrator

import java.io.File

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

import akka.actor.{Status => _, _}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.{Unstarted, Waiting}
import shapeless.ops.hlist.Tupler
import shapeless.{HList, HNil}

// TODO: we are identifying tasks by their description. This looks brittle. Maybe we should use the task Id to identify tasks

object ActorSysSpec {
  case object FinishOrchestrator
  case object OrchestratorFinished
  case object OrchestratorAborted
  case object GetDestinations
  case class Destinations(destinations: Map[String, TestProbe])
  
  val testsAbortReason = new Exception()
  
  abstract class ControllableOrchestrator(destinations: Array[TestProbe], startAndTerminateImmediately: Boolean = false)
    extends DistinctIdsOrchestrator {
    var destinationProbes = Map.empty[String, TestProbe]
  
    def fulltask[R, DL <: HList, RL <: HList, RP](description: String, destinationIndex: Int, message: Long => Serializable,
                                                  result: R, dependencies: DL = HNil: HNil,
                                                  timeout: Duration = Duration.Inf, abortOnReceive: Boolean = false)
                                                 (implicit orchestrator: AbstractOrchestrator[_],
                                                  cm: TaskComapped.Aux[DL, RL] = TaskComapped.nil,
                                                  tupler: Tupler.Aux[RL, RP] = Tupler.hnilTupler): FullTask[R, DL] = {
      destinationProbes += description -> destinations(destinationIndex)
      FullTask(description, dependencies, timeout) createTaskWith { _ =>
        new Task[R](_) {
          val destination: ActorPath = destinations(destinationIndex).ref.path
          def createMessage(id: Long): Serializable = message(id)
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
    }
    
    def simpleMessageFulltask[R, DL <: HList, RL <: HList, RP](description: String, destinationIndex: Int,
                                                               result: R = "finished", dependencies: DL = HNil: HNil,
                                                               timeout: Duration = Duration.Inf,
                                                               abortOnReceive: Boolean = false)
                                                              (implicit orchestrator: AbstractOrchestrator[_],
                                                               tc: TaskComapped.Aux[DL, RL] = TaskComapped.nil,
                                                               tupler: Tupler.Aux[RL, RP] = Tupler.hnilTupler): FullTask[R, DL] = {
      fulltask(description, destinationIndex, SimpleMessage, result, dependencies, timeout, abortOnReceive)
    }
  
    override def persistenceId: String = this.getClass.getSimpleName
    
    //No automatic snapshots
    override def saveSnapshotRoughlyEveryXMessages: Int = 0
  
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
        context become (computeCurrentBehavior() orElse crashable orElse {
          case FinishOrchestrator => terminate
        })
      }
    }
  
    def crashable: Receive = { case "boom" => throw new IllegalArgumentException("BOOM\n") with NoStackTrace }
    
    override def extraCommands: Receive = crashable orElse {
      case GetDestinations =>
        sender() ! Destinations(destinationProbes)
    }
  }
  
  // Quite ugly but makes the tests more readable
  implicit def state2SetOfState[S <: Task.State](s: S): Set[Task.State] = Set(s)
  implicit class RichTaskState(val tuple: (String, Task.State)) extends AnyVal {
    def or(other: Task.State): (String, Set[Task.State]) = {
      tuple match {
        case (taskDescription, firstState) => (taskDescription, Set(firstState, other))
      }
    }
  }
}

abstract class ActorSysSpec extends TestKit(ActorSystem())
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
  
  case class State(expectedStatus: Map[String, Set[Task.State]]) {
    def updatedStatuses(newStatuses: (String, Set[Task.State])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskDescription, possibleStatus)) =>
          statuses.updated(taskDescription, possibleStatus)
      }
      this.copy(expectedStatus = newExpectedStatus)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasks: Set[String]) {
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
    
    orchestratorActor.tell(GetDestinations, probe.ref)
    val testProbeOfTask = probe.expectMsgType[Destinations].destinations
    
    probe watch orchestratorActor
    
    val firstState: State = State(SortedMap.empty).updatedStatuses (
      startingTasks.toSeq.map(startingTask => (startingTask, Set[Task.State](Unstarted))):_*
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
    
    def pingPong(destination: TestProbe, ignoreTimeoutError: Boolean = false): Unit = {
      try {
        val m = destination.expectMsgType[SimpleMessage]
        logger.info(s"$m: ${destination.sender()} <-> ${destination.ref}")
        //system.actorSelection(destination.sender().path).tell(m, destination.ref)
        destination reply m
      } catch {
        case e: AssertionError if e.getMessage.contains("timeout") && ignoreTimeoutError =>
          // Purposefully ignored
      }
    }
    def pingPong(taskDescription: String): Unit = pingPong(testProbeOfTask(taskDescription))
    /** Alias to pingPong. But makes it obvious that we are handling a resend. */
    // TODO shouldn't handling a resend only be expecting the message? As opposed to expecting the message, then give an answer.
    def handleResend(destination: TestProbe, ignoreTimeoutError: Boolean = false): Unit = pingPong(destination, ignoreTimeoutError)
    def handleResend(taskDescription: String): Unit = pingPong(taskDescription)
    
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
        
        logger.info("=== Computing next state ===========================")
        val newState = transformation(lastState)
        logger.info("\n\n\n\n")
        newState
      }
    }
    
    def testStatus(state: State, max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, probe.ref)
      val taskState = probe.expectMsgType[StatusResponse](max)
        .tasks
        .map(t => (t.description, t.state))
        .toMap
      for((description, expected) <- state.expectedStatus) {
        // expected should contain (taskState(description))
        // does not work for the Aborted state because Exception does not implement its own equals.
        expected contains taskState(description)
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
    
    def expectInnerOrchestratorTermination(description: String, max: Duration = Duration.Undefined): Unit = {
      // We do not create Task{Quorum|Bundle}s via the fullTask of controllable orchestrator
      // so we cannot use the testProbeOfTask map to get the destination of the task
      orchestratorActor.tell(Status, probe.ref)
      probe.expectMsgType[StatusResponse].tasks.foreach {
        case Report(_, `description`, _, Task.Unstarted, _, _) =>
          throw new IllegalStateException("Cannot expect for inner orchestrator termination if the inner orchestrator hasn't started.")
        case Report(_, `description`, _, Task.Waiting, Some(destination), _) =>
          // To be able to watch an actor we need its ActorRef first
          system.actorSelection(destination).tell(Identify(1L), probe.ref)
          probe.expectMsgType[ActorIdentity] match {
            case ActorIdentity(1L, Some(ref)) =>
              // We know that when an orchestrator finishes or aborts it stops it self.
              // So we just need to expect for its termination.
              logger.info(s"Task $description destination ActorRef $ref")
              probe watch ref
              probe.expectTerminated(ref, max)
            case _ =>
              // The innerOrchestrator already finished, so we don't need to do anything
          }
        case _ =>
          // The task already finished or aborted
      }
    }
  }
}
