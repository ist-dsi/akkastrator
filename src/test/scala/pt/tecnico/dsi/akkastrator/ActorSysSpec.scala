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

class TestException(msg: String) extends Exception(msg) with NoStackTrace

object ActorSysSpec {
  case object Finish
  case object TerminatedEarly
  
  abstract class ControllableOrchestrator(terminationProbe: ActorRef, startAndTerminateImmediately: Boolean = false) extends Orchestrator {
    def task[R](description: String, _destination: ActorPath, _result: R, dependencies: Set[Task] = Set.empty[Task],
                earlyTermination: Boolean = false)(implicit orchestrator: AbstractOrchestrator) = {
      new Task(description, dependencies)(orchestrator) {
        type Result = R
        val destination: ActorPath = _destination
        def createMessage(id: Long): Any = SimpleMessage(description, id)
        
        def behavior: Receive = /*LoggingReceive.withLabel(f"Task [$index%02d - $description]")*/ {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            if (earlyTermination) {
              terminateEarly(m, id)
            } else {
              finish(m, id, _result)
            }
        }
      }
    }
    
    def echoTask(description: String, _destination: ActorPath, dependencies: Set[Task] = Set.empty[Task],
                 earlyTermination: Boolean = false)(implicit orchestrator: AbstractOrchestrator) = {
      task[String](description, _destination, "finished", dependencies, earlyTermination)(orchestrator)
    }
    
    def taskBundle[I](collection: ⇒ Seq[I], _destination: ActorPath, description: String, dependencies: Set[Task] = Set.empty) = {
      new TaskBundle(collection, description, dependencies) {
        type InnerResult = String
        
        def toTask(input: I) = {
          echoTask(s"$description-innerTask", _destination)(_)
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
    
    override def onEarlyTermination(instigator: Task, message: Any, tasks: Map[TaskState, Seq[Task]]): Unit = {
      log.info("Terminated Early")
      terminationProbe ! TerminatedEarly
    }
    
    //Add a case to always be able to crash the orchestrator
    override def extraCommands: Receive = {
      case "boom" ⇒ throw new TestException("BOOM")
    }
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
    val taskDestinations = statusProbe.expectMsgClass(classOf[StatusResponse]).tasks.zipWithIndex.collect {
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

    def pingPongDestinationOf(task: Symbol): Unit = {
      val destination = taskDestinations(task)
      val m = destination.expectMsgClass(classOf[SimpleMessage])
      logger.info(s"${destination.ref.path.name} received message $m")
      destination.reply(m)
    }
    
    def testStatus(expectedStatus: SortedMap[Int, Set[TaskState]], max: FiniteDuration = remainingOrDefault): Unit = {
      orchestratorActor.tell(Status, statusProbe.ref)
      val taskViews = statusProbe.expectMsgClass(max, classOf[StatusResponse]).tasks
      for((index, expected) <- expectedStatus) {
        expected should contain (taskViews(index).state)
      }
    }
  }
}
