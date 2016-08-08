package pt.tecnico.dsi.akkastrator

import java.io.File

import akka.actor.{ActorPath, ActorRef, ActorSystem, Props}
import akka.event.LoggingReceive
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.ActorSysSpec._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

object ActorSysSpec {
  case object FinishOrchestrator
  case object OrchestratorAborted
  
  abstract class ControllableOrchestrator(terminationProbe: ActorRef, startAndTerminateImmediately: Boolean = false)
    extends DistinctIdsOrchestrator {
    def task[R](description: String, _destination: ActorPath, _result: R, dependencies: Set[Task[_]] = Set.empty[Task[_]],
                abortOnReceive: Boolean = false)(implicit orchestrator: AbstractOrchestrator): Task[R] = {
      new Task[R](description, dependencies) {
        val destination: ActorPath = _destination
        def createMessage(id: Long): Any = SimpleMessage(description, id)
        
        def behavior: Receive = /*LoggingReceive.withLabel(f"Task [$index%02d - $description]")*/ {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            if (abortOnReceive) {
              abort(m, id)
            } else {
              finish(m, id, _result)
            }
        }
      }
    }
    
    def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task[_]] = Set.empty[Task[_]],
                 abortOnReceive: Boolean = false)(implicit orchestrator: AbstractOrchestrator): Task[String] = {
      task(description, _destination, "finished", dependencies, abortOnReceive)
    }
    
    def taskBundle[I](collection: ⇒ Seq[I], _destination: ActorPath, description: String, dependencies: Set[Task[_]] = Set.empty) = {
      new TaskBundle[I, String](collection, description, dependencies) {
        def toTask(input: I)(implicit orchestrator: AbstractOrchestrator): Task[String] = {
          task(s"$input", _destination, s"$input")
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
          case FinishOrchestrator ⇒ super.onFinish()
        })
      }
    }
    
    override def onAbort(instigator: Task[_], message: Any, tasks: Map[TaskState, Seq[Task[_]]]): Unit = {
      log.info("Aborted")
      terminationProbe ! OrchestratorAborted
    }
    
    //Add a case to always be able to crash the orchestrator
    override def extraCommands: Receive = {
      case "boom" ⇒ throw new IllegalArgumentException("BOOM") with NoStackTrace
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

  case class State(expectedStatus: SortedMap[Int, Set[TaskState]]) {
    def updatedStatuses(newStatuses: (Symbol, Set[TaskState])*): State = {
      val newExpectedStatus = newStatuses.foldLeft(expectedStatus) {
        case (statuses, (taskSymbol, possibleStatus)) ⇒
          statuses.updated(taskSymbol.name.head - 'A', possibleStatus)
      }
      this.copy(newExpectedStatus)
    }
    def updatedExactStatuses(newStatuses: (Symbol, TaskState)*): State = {
      val n = newStatuses.map { case (s, state) ⇒
        (s, Set(state))
      }
      updatedStatuses(n:_*)
    }
  }
  abstract class TestCase[O <: ControllableOrchestrator : ClassTag](numberOfDestinations: Int, startingTasks: Set[Symbol]) extends IdImplicits {
    val terminationProbe = TestProbe("termination-probe")
    val statusProbe = TestProbe("status-probe")
    val destinations = Array.tabulate(numberOfDestinations)(i ⇒ TestProbe(s"Dest-$i"))

    private val orchestratorClass = implicitly[ClassTag[O]].runtimeClass
    lazy val orchestratorActor = system.actorOf(
      Props(orchestratorClass, destinations, terminationProbe.ref),
      orchestratorClass.getSimpleName
    )
  
    orchestratorActor.tell(Status, statusProbe.ref)
    val destinationOfTask = statusProbe.expectMsgClass(classOf[StatusResponse]).tasks.zipWithIndex.collect {
      case (taskView, index) if destinations.indexWhere(_.ref.path == taskView.destination) >= 0 ⇒
        val destIndex = destinations.indexWhere(_.ref.path == taskView.destination)
        (Symbol(('A' + index).toChar.toString), destinations(destIndex))
    }.toMap
    
    terminationProbe.watch(orchestratorActor)

    val firstState: State = State(SortedMap.empty).updatedExactStatuses(startingTasks.toSeq.map((_, Unstarted)):_*)

    val firstTransformation: State ⇒ State = { s ⇒
      orchestratorActor ! StartReadyTasks
      val s = startingTasks.toSeq.map { s ⇒
        (s, Waiting(new DeliveryId(s.name.head - 'A' + 1)))
      }
      firstState.updatedExactStatuses(s:_*)
    }
    val transformations: Seq[State ⇒ State]
    val lastTransformation: State ⇒ State = { s ⇒
      orchestratorActor ! FinishOrchestrator
      s
    }

    private lazy val allTransformations = firstTransformation +: transformations :+ lastTransformation
  
    def pingPongDestinationOf(task: Symbol): Unit = {
      val destination = destinationOfTask(task)
      val m = destination.expectMsgClass(classOf[SimpleMessage])
      destination.reply(m)
      logger.info(s"$m: ${destination.sender().path.name} <-> ${destination.ref.path.name}")
    }
  
    //Performs the same test in each state
    def sameTestPerState(test: State ⇒ Unit): Unit = {
      var i = 1
      allTransformations.foldLeft(firstState) {
        case (lastState, transformationFunction) ⇒
          val expectedStateString = lastState.expectedStatus.mapValues(_.mkString(" OR ")).mkString("\n\t", "\n\t", "\n\t")
          logger.info(s"""\n\n\n=== STATE $i =============================
                          |EXPECTING:$expectedStateString""".stripMargin)
          test(lastState)
          i += 1
          logger.info(s"\n\n=== Computing next state ===========================")
          val newState = transformationFunction(lastState)
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
    
    def testStatus(expectedStatus: SortedMap[Int, Set[TaskState]], max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, statusProbe.ref)
      val taskViews = statusProbe.expectMsgClass(max, classOf[StatusResponse]).tasks
      for((index, expected) <- expectedStatus) {
        expected should contain (taskViews(index).state)
      }
    }
  
    def testRecovery(): Unit = sameTestPerState { state ⇒
      // Test if the orchestrator is in the expected state (aka the status is what we expect)
      testStatus(state.expectedStatus)
      // Crash the orchestrator
      orchestratorActor ! "boom"
      // Test that the orchestrator recovered to the expected state
      testStatus(state.expectedStatus)
    }
  }
}
