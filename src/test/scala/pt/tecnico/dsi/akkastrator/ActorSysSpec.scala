package pt.tecnico.dsi.akkastrator

import java.io.File

import akka.actor.{ActorIdentity, ActorPath, ActorRef, ActorSystem, Identify, Props}
import akka.event.LoggingReceive
import akka.persistence.RecoveryCompleted
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.Task.{Unstarted, Waiting}
import pt.tecnico.dsi.akkastrator.Orchestrator._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

object ActorSysSpec {
  case object FinishOrchestrator
  case object OrchestratorAborted
  
  val testsAbortReason = OhSnap("I was pre-programmed to abort")
  
  abstract class ControllableOrchestrator(terminationProbe: ActorRef, startAndTerminateImmediately: Boolean = false)
    extends DistinctIdsOrchestrator {
    def task[R](description: String, _destination: ActorPath, _result: R, dependencies: Set[Task[_]] = Set.empty,
                abortOnReceive: Boolean = false)(implicit orchestrator: AbstractOrchestrator[_]): Task[R] = {
      new Task[R](description, dependencies) {
        val destination: ActorPath = _destination
        def createMessage(id: Long): Any = SimpleMessage(description, id)
        
        def behavior: Receive = /*LoggingReceive.withLabel(f"Task [$index%02d - $description]")*/ {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            if (abortOnReceive) {
              abort(m, testsAbortReason, id)
            } else {
              finish(m, id, _result)
            }
        }
      }
    }
    
    def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task[_]] = Set.empty,
                 abortOnReceive: Boolean = false)(implicit orchestrator: AbstractOrchestrator[_]): Task[String] = {
      task(description, _destination, "finished", dependencies, abortOnReceive)
    }
  
    override def persistenceId: String = this.getClass.getSimpleName
    
    //No automatic snapshots
    override def saveSnapshotRoughlyEveryXMessages: Int = 0
  
    if (startAndTerminateImmediately) {
      self ! StartOrchestrator(1L)
    }
    
    override def onFinish(): Unit = {
      if (startAndTerminateImmediately) {
        super.onFinish()
      } else {
        // We include the orchestratorCommand because we still want to handle Status messages.
        // We include the extraCommands because we still want to be able to crash the orchestrator
        context become (orchestratorCommand orElse crashable orElse finishable(super.onFinish()))
      }
    }
  
    override def onAbort(instigator: Task[_], message: Any, cause: AbortCause, tasks: Map[Task.State, Seq[Task[_]]]): Unit = {
      terminationProbe ! OrchestratorAborted
      
      if (startAndTerminateImmediately) {
        super.onAbort(instigator, message, cause, tasks)
      } else {
        context become (computeCurrentBehavior() orElse finishable(super.onAbort(instigator, message, cause, tasks)))
      }
    }
  
    def finishable(afterFinish: => Unit): Receive = {
      case FinishOrchestrator => afterFinish
    }
    def crashable: Receive = {
      case "boom" ⇒ throw new IllegalArgumentException("BOOM") with NoStackTrace
    }
    
    //Add a case to always be able to crash the orchestrator
    override def extraCommands: Receive = crashable
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

  case class State(expectedStatus: SortedMap[String, Set[Task.State]]) {
    def updatedStatuses(newStatuses: (String, Set[Task.State])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskDescription, possibleStatus)) ⇒
          statuses.updated(taskDescription, possibleStatus)
      }
      this.copy(expectedStatus = newExpectedStatus)
    }
    def updatedExactStatuses(newStatuses: (String, Task.State)*): State = {
      val n = newStatuses.map { case (s, state) ⇒
        (s, Set(state))
      }
      updatedStatuses(n:_*)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasks: Set[String]) {
    val terminationProbe = TestProbe("termination-probe")
    val statusProbe = TestProbe("status-probe")
    val destinations = Array.fill(numberOfDestinations)(TestProbe("dest"))
    
    private val orchestratorClass = implicitly[ClassTag[O]].runtimeClass
    val orchestratorActor = system.actorOf(
      Props(orchestratorClass, destinations, terminationProbe.ref),
      orchestratorClass.getSimpleName
    )
    
    orchestratorActor.tell(Status, statusProbe.ref)
    private val initialTaskReports = statusProbe.expectMsgClass(classOf[StatusResponse]).tasks
    val testProbeOfTask = initialTaskReports.collect {
      case taskView if destinations.indexWhere(_.ref.path == taskView.destination) >= 0 =>
        val destIndex = destinations.indexWhere(_.ref.path == taskView.destination)
        (taskView.description, destinations(destIndex))
    }.toMap
    
    val destinationOfTask = initialTaskReports.map(t => (t.description, t.destination)).toMap
    
    terminationProbe.watch(orchestratorActor)
    
    val firstState: State = State(SortedMap.empty).updatedExactStatuses (
      startingTasks.toSeq.map(startingTask ⇒ (startingTask, Unstarted)):_*
    )
    
    val firstTransformation: State ⇒ State = { s ⇒
      logger.info(s"Starting the Orchestrator")
      orchestratorActor ! StartOrchestrator(1L)
      val s = startingTasks.toSeq.map { s ⇒
        (s, Set[Task.State](Unstarted, Waiting))
      }
      firstState.updatedStatuses(s:_*)
    }
    val transformations: Seq[State ⇒ State]
    val lastTransformation: State ⇒ State = { s ⇒
      logger.info("Sending FinishOrchestrator")
      orchestratorActor ! FinishOrchestrator
      s
    }
    
    private lazy val allTransformations = firstTransformation +: transformations :+ lastTransformation
    
    def pingPong(destination: TestProbe): Unit = {
      val m = destination.expectMsgClass(classOf[SimpleMessage])
      logger.info(s"$m: ${destination.sender().path} <-> ${destination.ref}")
      destination.reply(m)
      /*logger.info(s"${destination.ref} received $m from ${destination.lastSender}")
      system.actorSelection(destination.lastSender.path).tell(m, destination.ref)
      logger.info(s"${destination.ref} sent $m to ${destination.sender().path}")*/
    }
    def pingPongTestProbeOf(taskDescription: String): Unit = pingPong(testProbeOfTask(taskDescription))
    
    def sameTestPerState(test: State ⇒ Unit): Unit = {
      var i = 1
      allTransformations.foldLeft(firstState) {
        case (lastState, transformationFunction) ⇒
          val expectedStateString = lastState.expectedStatus.mapValues(_.mkString(" | ")).mkString("\n\t", "\n\t", "\n\t")
          logger.info(s"""=== STATE $i =============================
                          |EXPECTING:$expectedStateString""".stripMargin)
          test(lastState)
          i += 1
          logger.info("=== Computing next state ===========================")
          val newState = transformationFunction(lastState)
          logger.info("\n\n\n\n")
          newState
      }
      //terminationProbe.expectTerminated(orchestratorActor)
    }
    
    def differentTestPerState(tests: (State ⇒ Unit)*): Unit = {
      val testsAndTransformations: Seq[(State ⇒ Unit, State ⇒ State)] = tests.zip(allTransformations)
      testsAndTransformations.foldLeft(firstState) {
        case (lastState, (test, transformation)) ⇒
          //Perform the test for lastState
          test(lastState)

          transformation(lastState)
      }
    }
    
    def testStatus(state: State, max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, statusProbe.ref)
      val taskViews = statusProbe.expectMsgClass(max, classOf[StatusResponse]).tasks.map(t => (t.description, t)).toMap
      for((description, expected) <- state.expectedStatus) {
        val currentState = taskViews(description).state
        expected should contain (currentState)
      }
    }
    
    def testExpectedStatusWithRecovery(max: FiniteDuration = 1.minute): Unit = sameTestPerState { state ⇒
      // Test if the orchestrator is in the expected state (aka the status is what we expect)
      testStatus(state)
      // Crash the orchestrator
      orchestratorActor ! "boom"
      // Test that the orchestrator recovered to the expected state
      testStatus(state)
    }
    
    def expectInnerOrchestratorTermination(description: String, max: Duration = Duration.Undefined): Unit = {
      // To be able to watch an actor we need its ActorRef first
      system.actorSelection(destinationOfTask(description)).tell(Identify(1L), terminationProbe.ref)
      terminationProbe.expectMsgClass(classOf[ActorIdentity]) match {
        case ActorIdentity(_, Some(ref)) =>
          // We know the inner orchestrator will terminate because that is what the TaskSpawnOrchestrator contract states.
          // In reality we will be watching the Spawner and not the innerOrchestrator directly, but since the spawner is
          // a proxy to the innerOrchestrator this works out to the same thing as watching the innerOrchestrator directly.
          terminationProbe.watch(ref)
          terminationProbe.expectTerminated(ref, max)
        case _ =>
          // The innerOrchestrator already finished, so we don't need to do anything
      }
    }
  }
}
