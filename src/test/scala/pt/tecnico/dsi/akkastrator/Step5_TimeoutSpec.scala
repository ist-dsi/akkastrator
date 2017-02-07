package pt.tecnico.dsi.akkastrator

import java.util.concurrent.TimeoutException

import scala.concurrent.duration.DurationInt

import akka.actor.ActorPath
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.Orchestrator.TaskAborted
import pt.tecnico.dsi.akkastrator.Step5_TimeoutSpec._
import pt.tecnico.dsi.akkastrator.Task.{Aborted, Finished, Timeout}

object Step5_TimeoutSpec {
  class ExplicitTimeoutHandling(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    destinationProbes += "A" -> destinations(0)
    FullTask("A", timeout = 0.millis) createTaskWith { _ =>
      new Task[String](_) {
        val destination: ActorPath = destinations(0).ref.path
        def createMessage(id: Long): Serializable = SimpleMessage("A", id)
        def behavior: Receive =  {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "A Result")
          case m @ Timeout(id) if matchId(id) =>
            finish(m, id, "A special error message")
        }
      }
    }
  }
  class AutomaticTimeoutHandling(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    destinationProbes += "A" -> destinations(0)
    FullTask("A", timeout = 0.millis) createTaskWith { _ =>
      new Task[String](_) {
        val destination: ActorPath = destinations(0).ref.path
        def createMessage(id: Long): Serializable = SimpleMessage("A", id)
        def behavior: Receive =  {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "A Result")
        }
      }
    }
  }
}
class Step5_TimeoutSpec extends ActorSysSpec {
  //Test:
  // Timeout = Duration.Inf => does not cause any timeout
  // Timeout = FiniteDuration causes a timeout, sending a Task.Timeout to the task behavior.
  //  · If the task handles that message, check that it is correctly handled
  //  · If the task does not handle it then check if the task aborts with cause = TimeoutException
  // Timeouts inside inner orchestrators are tested in their own suites.
  
  // The case where timeout = Duration.Inf cannot be tested since we can't wait forever.
  // However all the other tests prove the timeout is not "thrown" when it is set as Duration.Inf
  
  "A orchestrator with timeouts" should {
    "execute the behavior" when {
      "it handles the Timeout message" in {
        val testCase1 = new TestCase[ExplicitTimeoutHandling](1, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              testProbeOfTask("A") expectMsgClass classOf[SimpleMessage]
              // We purposefully do not reply
              
              secondState.updatedStatuses(
                "A" -> Finished("A special error message")
              )
            }
          )
        }
        testCase1.testExpectedStatusWithRecovery()
      }
    }
    "abort" when {
      "behavior does not handle the Timeout message" in {
        val testCase2 = new TestCase[AutomaticTimeoutHandling](1, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              testProbeOfTask("A") expectMsgClass classOf[SimpleMessage]
              // We purposefully do not reply
              
              secondState.updatedStatuses(
                "A" -> Aborted(new TimeoutException())
              )
            }
          )
        }
        import testCase2._
        differentTestPerState(
          { testStatus(_) }, // 1st state: startingTasks -> Unstarted.
          { testStatus(_) }, // 2nd state: startingTasks -> Unstarted | Waiting.
          { thirdState =>
            // Ensure task A aborted
            testStatus(thirdState)
    
            // The default implementation of onTaskAborted calls onAbort, which in the controllable orchestrator
            // sends the message OrchestratorAborted to its parent.
            parentProbe expectMsg OrchestratorAborted
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Task.Report("A", Seq(), Aborted(_: TimeoutException), _, None), _: TimeoutException, _) => true
            }
          }
        )
      }
    }
  }
}
