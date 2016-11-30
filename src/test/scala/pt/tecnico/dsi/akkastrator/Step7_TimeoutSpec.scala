package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorPath, ActorRef}
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.Step7_TimeoutSpec._
import pt.tecnico.dsi.akkastrator.Task.{Aborted, Finished, Timeout}

object Step7_TimeoutSpec {
  class ExplicitTimeoutHandlingOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    destinationProbes += "A" -> destinations(0)
    FullTask("A", timeout = 500.millis) createTaskWith { _ =>
      new Task[String](_) {
        val destination: ActorPath = destinations(0).ref.path
        def createMessage(id: Long): Any = SimpleMessage("A", id)
        def behavior: Receive =  {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "A Result")
          case m @ Timeout(id) =>
            finish(m, id, "A special error message")
        }
      }
    }
  }
  class AutomaticTimeoutHandlingOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    destinationProbes += "A" -> destinations(0)
    FullTask("A", timeout = 500.millis) createTaskWith { _ =>
      new Task[String](_) {
        val destination: ActorPath = destinations(0).ref.path
        def createMessage(id: Long): Any = SimpleMessage("A", id)
        def behavior: Receive =  {
          case m @ SimpleMessage(_, id) if matchId(id) =>
            finish(m, id, "A Result")
        }
      }
    }
  }
}
class Step7_TimeoutSpec extends ActorSysSpec {
  //Test:
  // Timeout = Duration.Inf => does not cause any timeout
  // Timeout = FiniteDuration causes a timeout, sending a Task.Timeout to the task behavior.
  //  · If the task handles that message, check it is correctly handled
  //  · If the task does not handle that message then check the task aborts with cause = TimedOut
  // Timeouts inside inner orchestrators is tested in their own suites.
  
  // The case where timeout = Duration.Inf cannot be tested since we can't wait forever.
  // However all the other tests prove the timeout is not "throw" when it is set as Duration.Inf
  
  "A orchestrator with timeouts" should {
    "execute the behavior" when {
      "it handles the Timeout message" in {
        val testCase1 = new TestCase[ExplicitTimeoutHandlingOrchestrator](1, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              testProbeOfTask("A") expectMsgClass classOf[SimpleMessage]
              // We purposefully do not reply
              
              // Ensure the timeout is triggered
              Thread.sleep(600)
              
              secondState.updatedExactStatuses(
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
        val testCase2 = new TestCase[AutomaticTimeoutHandlingOrchestrator](1, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              testProbeOfTask("A") expectMsgClass classOf[SimpleMessage]
              // We purposefully do not reply
              
              // Ensure the timeout is triggered
              Thread.sleep(600)
              
              secondState.updatedExactStatuses(
                "A" -> Aborted(TimedOut)
              )
            }
          )
        }
        testCase2.testExpectedStatusWithRecovery()
      }
    }
  }
}
