package pt.tecnico.dsi.akkastrator

import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.Orchestrator.TaskAborted
import pt.tecnico.dsi.akkastrator.Step4_AbortSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object Step4_AbortSpec {
  class AbortSingleTask(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    // A
    simpleMessageFulltask("A", 0, abortOnReceive = true)
  }
  class AbortTwoIndependentTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    // A
    // B
    simpleMessageFulltask("A", 0, abortOnReceive = true)
    simpleMessageFulltask("B", 1)
  }
  class AbortTwoLinearTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    // A → B
    val a = simpleMessageFulltask("A", 0, abortOnReceive = true)
    simpleMessageFulltask("B", 1, dependencies = a :: HNil)
  }
}
class Step4_AbortSpec extends ActorSysSpec {
  // Ensure the following happens:
  //  1. The task that aborted will change its state to `Aborted`.
  //  2. Every unstarted task that depends on the aborted task will never be started.
  //  3. Waiting tasks or tasks which do not have the aborted task as a dependency will remain untouched.
  //  4. The method `onTaskAbort` will be invoked in the orchestrator.
  //  5. The method `onFinish` in the orchestrator will never be invoked since this task did not finish.
  
  "An orchestrator with tasks that abort" should {
    "behave according to the documentation" when {
      "there is only a single task: A" in {
        val testCase = new TestCase[AbortSingleTask](1, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Aborted(testsAbortReason)
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
              case TaskAborted(Report(0, "A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      "there are two independent tasks: A B" in {
        val testCase = new TestCase[AbortTwoIndependentTasks](2, Set(0, 1)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Aborted(testsAbortReason),
                1 -> Waiting
              )
            }, { thirdState =>
              pingPong(destinations(1)) // Destination of Task "B"

              thirdState.updatedStatuses(
                1 -> Finished("finished")
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
            parentProbe.expectNoMessage()
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Report(0, "A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      "there are two dependent tasks: A → B" in {
        val testCase = new TestCase[AbortTwoLinearTasks](2, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Aborted(testsAbortReason)
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
            destinations(1).expectNoMessage() // Destination of Task "B"
  
            // Confirm A -> Aborted and B -> Unstarted
            testStatus(thirdState)
          }, { _ =>
            // Confirm that the orchestrator has indeed aborted
            parentProbe.expectMsgPF() {
              case TaskAborted(Report(0, "A", Seq(), Aborted(`testsAbortReason`), _, None), `testsAbortReason`, _) => true
            }
          }
        )
      }
      
      // Tests for tasks aborting inside a Task{Bundle,Quorum} are performed in those test suites
    }
  }
}