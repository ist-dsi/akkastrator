package pt.tecnico.dsi.akkastrator

import java.io.File

import akka.actor.Actor._
import akka.actor.{ActorPath, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.persistence.DeleteMessagesSuccess
import akka.testkit.{TestDuration, TestKit, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest._
import pt.tecnico.dsi.akkastrator.Orchestrator.CorrelationId
import pt.tecnico.dsi.akkastrator.Task.Status
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

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
  }


  abstract class SnapshotlessOrchestrator extends Orchestrator {
    //No snapshots
    override def saveSnapshotEveryXMessages: Int = 0

    def persistenceId: String = this.getClass.getSimpleName

    override def onFinish(): Unit = {
      deleteMessages(lastSequenceNr)
      context become LoggingReceive {
        case DeleteMessagesSuccess(toSequenceNr) ⇒
          log.info("Orchestrator Finished!")
          context.stop(self)
      }
    }
  }

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

  def NChainedTasksOrchestrator(numberOfTasks: Int): (Array[TestProbe], ActorRef) = {
    require(numberOfTasks >= 2, "Must have at least 2 tasks")
    val destinations = Array.fill(numberOfTasks)(TestProbe())

    val letters = 'A' to 'Z'
    val orchestrator = system.actorOf(Props(new SnapshotlessOrchestrator {
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

    withOrchestratorTermination(orchestrator, (numberOfTasks * 300).millis.dilated) { _ =>
      for (i <- 0 until numberOfTasks) {
        val message = destinations(i).expectMsgClass(200.millis.dilated, classOf[SimpleMessage])
        for (j <- (i + 1) until numberOfTasks) {
          destinations(j).expectNoMsg(100.millis.dilated)
        }
        destinations(i).reply(SimpleMessage(message.id))
      }
    }
  }

  def withOrchestratorTermination(orchestrator: ActorRef, maxDuration: Duration = 2.second.dilated)(f: TestProbe => Unit): Unit = {
    val probe = TestProbe()
    probe.watch(orchestrator)
    f(probe)
    probe.expectMsgPF(maxDuration){ case Terminated(o) if o == orchestrator => true }
  }

  def testStatus[T](orchestrator: ActorRef, probe: TestProbe, maxDuration: FiniteDuration = 500.millis.dilated)
                   (expectedStatus: SortedMap[Int, Set[Task.Status]]): Unit = {
    orchestrator.tell(Status, probe.ref)
    val obtainedStatus: IndexedSeq[Status] = probe.expectMsgClass(maxDuration, classOf[StatusResponse]).tasks.map(_.status).toIndexedSeq

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
}
