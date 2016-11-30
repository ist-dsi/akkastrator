package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.Step4_AbortSpec._
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, testsAbortReason}
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object Step4_AbortSpec {
  class AbortSingleTaskOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
  }
  class AbortTwoIndependentTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    // B
    simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
    simpleMessageFulltask("B", destinations(1))
  }
  class AbortTwoLinearTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    val a = simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
    simpleMessageFulltask("B", destinations(1), dependencies = a :: HNil)
  }
}
class Step4_AbortSpec extends ActorSysSpec {
  //Ensure the following happens:
  //  · The task that instigated the abort will change state to Aborted.
  //  · Every unstarted task that depends on this one will never be started.
  //  · Waiting tasks or tasks which do not have this task as a dependency will continue to be executed, unless
  //    the orchestrator is stopped.
  //  · The method `onAbort` will be invoked in the orchestrator.
  //  · The method `onFinish` will NEVER be called even if the only tasks needed to finish
  //    the orchestrator are already waiting and the responses are received.

  "An orchestrator with tasks that abort" should {
    "behave according to the documentation" when {
      "there is only a single task: A" in {
        val testCase = new TestCase[AbortSingleTaskOrchestrator](1, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")

              secondState.updatedExactStatuses(
                "A" → Aborted(testsAbortReason)
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState => () }, // We don't want to test anything for this state
          { secondState => () }, // We don't want to test anything for this state
          { thirdState =>
            testStatus(thirdState)
            //Confirm we received the OrchestratorAborted
            terminationProbe.expectMsg(ActorSysSpec.OrchestratorAborted)
            //Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()
          }
        )
      }
      "there are two independent tasks: A B" in {
        val testCase = new TestCase[AbortTwoIndependentTasksOrchestrator](2, Set("A", "B")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")

              secondState.updatedExactStatuses(
                "A" → Aborted(testsAbortReason),
                "B" → Waiting
              )
            }, { thirdState =>
              pingPong("B")

              thirdState.updatedExactStatuses(
                "B" → Finished("finished")
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState => () }, // We don't want to test anything for this state
          { secondState => () }, // We don't want to test anything for this state
          { thirdState =>
            testStatus(thirdState)
            // Confirm we received the OrchestratorAborted
            terminationProbe.expectMsg(ActorSysSpec.OrchestratorAborted)
            // Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()
          }, { fourthState =>
            // This confirms that when aborting, tasks that are waiting will remain untouched
            // and the orchestrator will still be prepared to handle their responses
            testStatus(fourthState)

            // This confirms the method `onFinish` will NEVER be called even if the only tasks needed to finish
            // the orchestrator (aka Task B) are already waiting and its response is received.
            terminationProbe.expectNoMsg()
          }
        )
      }
      "there are two dependent tasks: A → B" in {
        val testCase = new TestCase[AbortTwoLinearTasksOrchestrator](2, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")

              secondState.updatedExactStatuses(
                "A" → Aborted(testsAbortReason)
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState => () }, // We don't want to test anything for this state
          { secondState => () }, // We don't want to test anything for this state
          { thirdState =>
            testStatus(thirdState)
            // Confirm we received the OrchestratorAborted
            terminationProbe.expectMsg(ActorSysSpec.OrchestratorAborted)
            // Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()

            // Confirm that every unstarted task (task B) will be prevented from starting even if its dependencies have finished.
            testProbeOfTask("B").expectNoMsg()
          }
        )
      }
      
      // Tests for tasks aborting inside a Task{Bundle,Quorum} are performed in those test suites
    }
  }
}