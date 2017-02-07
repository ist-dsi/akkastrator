package pt.tecnico.dsi.akkastrator

import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.Orchestrator.TaskAborted
import pt.tecnico.dsi.akkastrator.Step4_AbortSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object Step4_AbortSpec {
  class AbortSingleTask(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    // A
    simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
  }
  class AbortTwoIndependentTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    // A
    // B
    simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
    simpleMessageFulltask("B", destinations(1))
  }
  class AbortTwoLinearTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    // A → B
    val a = simpleMessageFulltask("A", destinations(0), abortOnReceive = true)
    simpleMessageFulltask("B", destinations(1), dependencies = a :: HNil)
  }
}
class Step4_AbortSpec extends ActorSysSpec {
  //Ensure the following happens:
  // 1. The task that aborted will change its state to `Aborted`.
  // 2. Every unstarted task that depends on the aborted task will never be started.
  // 3. Waiting tasks or tasks which do not have the aborted task as a dependency will remain untouched.
  // 4. The method `onTaskAbort` will be invoked in the orchestrator.
  // 5. The method `onFinish` in the orchestrator will never be invoked since this task did not finish.
  
  "An orchestrator with tasks that abort" should {
    "behave according to the documentation" when {
      "there is only a single task: A" in {
        val testCase = new TestCase[AbortSingleTask](1, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
                "A" -> Aborted(testsAbortReason)
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { testStatus(_) }, // 1st state: startingTasks -> Unstarted.
          { testStatus(_) }, // 2nd state: startingTasks -> Unstarted | Waiting.
          { thirdState =>
            // Ensure tasks have the correct state
            testStatus(thirdState)
            
            // The default implementation of onTaskAborted calls onAbort, which in the controllable orchestrator
            // sends the message OrchestratorAborted to its parent.
            parentProbe expectMsg OrchestratorAborted
            
            // If onFinish was called then parentProbe would receive an OrchestratorFinish instead of OrchestratorAborted.
            // So we are also testing that onFinish is not called
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Task.Report("A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      "there are two independent tasks: A B" in {
        val testCase = new TestCase[AbortTwoIndependentTasks](2, Set("A", "B")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
                "A" -> Aborted(testsAbortReason),
                "B" -> Waiting
              )
            }, { thirdState =>
              pingPong("B")

              thirdState.updatedStatuses(
                "B" -> Finished("finished")
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { testStatus(_) }, // 1st state: startingTasks -> Unstarted.
          { testStatus(_) }, // 2nd state: startingTasks -> Unstarted | Waiting.
          { thirdState =>
            // Ensure tasks have the correct state
            testStatus(thirdState)
            parentProbe expectMsg OrchestratorAborted
            
            // Because the orchestrator does not abort right away the task will still continue execution.
            // But this is only true because we are using the ControllableOrchestrator.
          }, { fourthState =>
            // This confirms that when aborting, tasks that are waiting will remain untouched
            // and the orchestrator will still be prepared to handle their responses. Aka task B still finishes
            testStatus(fourthState)
    
            // This confirms the method `onFinish` will NEVER be called even if the only tasks needed to finish
            // the orchestrator (aka Task B) are already waiting and its response is received.
            parentProbe.expectNoMsg()
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Task.Report("A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      "there are two dependent tasks: A → B" in {
        val testCase = new TestCase[AbortTwoLinearTasks](2, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
                "A" -> Aborted(testsAbortReason)
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { testStatus(_) }, // 1st state: startingTasks -> Unstarted.
          { testStatus(_) }, // 2nd state: startingTasks -> Unstarted | Waiting.
          { thirdState =>
            parentProbe expectMsg OrchestratorAborted

            // This confirms that every unstarted task that depends on the aborted task will never be started
            testProbeOfTask("B").expectNoMsg()
  
            // Confirm A -> Aborted and B -> Unstarted
            testStatus(thirdState)
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Task.Report("A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      
      // Tests for tasks aborting inside a Task{Bundle,Quorum} are performed in those test suites
    }
  }
}