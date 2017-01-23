package pt.tecnico.dsi.akkastrator

import java.io.File

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

import akka.actor.{ActorIdentity, ActorPath, ActorRef, ActorSystem, Identify, Props}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import pt.tecnico.dsi.akkastrator.Orchestrator._
import pt.tecnico.dsi.akkastrator.Task.{Unstarted, Waiting}
import shapeless.{HList, HNil}

object ActorSysSpec {
  case object FinishOrchestrator
  case object OrchestratorAborted
  case object GetDestinations
  case class Destinations(d: Map[String, TestProbe])
  
  val testsAbortReason = new Exception()
  
  abstract class ControllableOrchestrator(terminationProbe: ActorRef, startAndTerminateImmediately: Boolean = false)
    extends DistinctIdsOrchestrator {
    var destinationProbes = Map.empty[String, TestProbe]
  
    def fulltask[R, DL <: HList, RL <: HList](description: String, dest: TestProbe, message: Long => Serializable, _result: R,
                                              dependencies: DL = HNil: HNil, abortOnReceive: Boolean = false)
                                             (implicit orchestrator: AbstractOrchestrator[_],
                                              tc: TaskComapped.Aux[DL, RL] = TaskComapped.nil): FullTask[R, DL] = {
      destinationProbes += description -> dest
      FullTask(description, dependencies) createTask { _ =>
        new Task[R](_) {
          val destination: ActorPath = dest.ref.path
          def createMessage(id: Long): Serializable = message(id)
          def behavior: Receive =  {
            case m @ SimpleMessage(_, id) if matchId(id) =>
              if (abortOnReceive) {
                abort(m, id, testsAbortReason)
              } else {
                finish(m, id, _result)
              }
          }
        }
      }
    }
    
    def simpleMessageFulltask[R, DL <: HList, RL <: HList](description: String, dest: TestProbe, _result: R = "finished",
                                                           dependencies: DL = HNil: HNil, abortOnReceive: Boolean = false)
                                                          (implicit orchestrator: AbstractOrchestrator[_],
                                                           tc: TaskComapped.Aux[DL, RL] = TaskComapped.nil): FullTask[R, DL] = {
      fulltask(description, dest, SimpleMessage(description, _), _result, dependencies, abortOnReceive)
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
        context become (alwaysAvailableCommands orElse crashable orElse finishable(super.onFinish()))
      }
    }
  
    override def onAbort(instigator: FullTask[_, _], message: Any, cause: Exception, tasks: Map[Task.State, Seq[FullTask[_, _]]]): Unit = {
      terminationProbe ! OrchestratorAborted
      
      if (startAndTerminateImmediately) {
        super.onAbort(instigator, message, cause, tasks)
      } else {
        context become (computeCurrentBehavior() orElse finishable(super.onAbort(instigator, message, cause, tasks)))
      }
    }
  
    def finishable(afterFinish: => Unit): Receive = { case FinishOrchestrator => afterFinish }
    def crashable: Receive = { case "boom" => throw new IllegalArgumentException("BOOM") with NoStackTrace }
    
    override def extraCommands: Receive = crashable orElse {
      case GetDestinations =>
        sender() ! Destinations(destinationProbes)
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

  case class State(expectedStatus: SortedMap[String, Set[Task.State]]) {
    def updatedStatuses(newStatuses: (String, Set[Task.State])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskDescription, possibleStatus)) =>
          statuses.updated(taskDescription, possibleStatus)
      }
      this.copy(expectedStatus = newExpectedStatus)
    }
    def updatedExactStatuses(newStatuses: (String, Task.State)*): State = {
      val n = newStatuses.map { case (s, state) =>
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
    
    orchestratorActor.tell(GetDestinations, statusProbe.ref)
    val testProbeOfTask = statusProbe.expectMsgClass(classOf[Destinations]).d
    
    terminationProbe.watch(orchestratorActor)
    
    val firstState: State = State(SortedMap.empty).updatedExactStatuses (
      startingTasks.toSeq.map(startingTask => (startingTask, Unstarted)):_*
    )
    
    val firstTransformation: State => State = { s =>
      logger.info(s"Starting the Orchestrator")
      orchestratorActor ! StartOrchestrator(1L)
      val s = startingTasks.toSeq.map { s =>
        (s, Set[Task.State](Unstarted, Waiting))
      }
      firstState.updatedStatuses(s:_*)
    }
    val transformations: Seq[State => State]
    val lastTransformation: State => State = { s =>
      logger.info("Sending FinishOrchestrator")
      orchestratorActor ! FinishOrchestrator
      s
    }
    
    private lazy val allTransformations = firstTransformation +: transformations :+ lastTransformation
    
    def pingPong(destination: TestProbe): Unit = {
      val m = destination expectMsgClass classOf[SimpleMessage]
      logger.info(s"$m: ${destination.sender().path} <-> ${destination.ref}")
      destination reply m
      /*logger.info(s"${destination.ref} received $m from ${destination.lastSender}")
      system.actorSelection(destination.lastSender.path).tell(m, destination.ref)
      logger.info(s"${destination.ref} sent $m to ${destination.sender().path}")*/
    }
    def pingPong(taskDescription: String): Unit = pingPong(testProbeOfTask(taskDescription))
    def handleResend(taskDescription: String): Unit = pingPong(taskDescription)
    
    def sameTestPerState(test: State => Unit): Unit = {
      var i = 1
      allTransformations.foldLeft(firstState) {
        case (lastState, transformationFunction) =>
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
    
    def differentTestPerState(tests: (State => Unit)*): Unit = {
      val testsAndTransformations: Seq[(State => Unit, State => State)] = tests.zip(allTransformations)
      testsAndTransformations.foldLeft(firstState) {
        case (lastState, (test, transformation)) =>
          //Perform the test for lastState
          test(lastState)

          transformation(lastState)
      }
    }
    
    def testStatus(state: State, max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, statusProbe.ref)
      val taskState = statusProbe.expectMsgClass(max, classOf[StatusResponse])
        .tasks
        .map(t => (t.description, t.state))
        .toMap
      for((description, expected: Set[Task.State]) <- state.expectedStatus) {
        //Cant understand why but using expected should contain (taskState(description))
        //and having an Aborted(new Exception) make this line fail when it shouldn't
        expected.contains(taskState(description))
      }
    }
    
    def testExpectedStatusWithRecovery(max: FiniteDuration = 30.seconds): Unit = sameTestPerState { state =>
      // Test if the orchestrator is in the expected state (aka the status is what we expect)
      testStatus(state)
      // Crash the orchestrator
      orchestratorActor ! "boom"
      // Test that the orchestrator recovered to the expected state
      testStatus(state)
    }
    
    def expectInnerOrchestratorTermination(description: String, max: Duration = Duration.Undefined): Unit = {
      orchestratorActor.tell(Status, statusProbe.ref)
      val dests = statusProbe.expectMsgClass(classOf[StatusResponse]).tasks.map(t => (t.description, t.destination)).toMap
      
      dests.get(description).flatten.foreach { dest =>
        // To be able to watch an actor we need its ActorRef first
        system.actorSelection(dest).tell(Identify(1L), terminationProbe.ref)
        terminationProbe.expectMsgClass(classOf[ActorIdentity]) match {
          case ActorIdentity(_, Some(ref)) =>
            // We know the inner orchestrator will terminate because that is what the TaskSpawnOrchestrator contract states.
            // In reality we will be watching the Spawner and not the innerOrchestrator directly, but since the spawner is
            // a proxy to the innerOrchestrator this works out to the same thing as watching the innerOrchestrator directly.
            logger.error(s"Task $description destination ActorRef $ref")
            terminationProbe.watch(ref)
            terminationProbe.expectTerminated(ref)
          case _ =>
            // The innerOrchestrator already finished, so we don't need to do anything
            logger.error(s"Could not get an ActorRef for the destination of task $description")
        }
      }
    }
  }
}
