package pt.tecnico.dsi.akkastrator

import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.TestCaseOrchestrators._

class EarlyTerminationSpec extends ActorSysSpec {
  //Ensure the following happens:
  //  · The task that instigated the early termination will be finished.
  //  · Every unstarted task will be prevented from starting even if its dependencies have finished.
  //  · Tasks that are waiting will remain untouched and the orchestrator will
  //    still be prepared to handle their responses.
  //  · The method `onEarlyTermination` will be invoked in the orchestrator.
  //  · The method `onFinish` will NEVER be called even if the only tasks needed to finish
  //    the orchestrator are already waiting and the responses are received.

  "An orchestrator that terminates early" should {
    "behave according to the documentation" when {
      "there is only a single task: A" in {
        val testCase = new TestCase[EarlyStopSingleTaskOrchestrator](1, Set('A)) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationNumber(0)

              secondState.updatedExactStatuses(
                'A -> Finished
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState ⇒ () }, // We don't want to test anything for this state
          { secondState ⇒ () }, // We don't want to test anything for this state
          { thirdState ⇒
            testStatus(orchestratorActor, statusProbe)(thirdState.expectedStatus)
            //Confirm we received the TerminatedEarly
            terminationProbe.expectMsg(TerminatedEarly)
            //Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()
            }
        )
      }
      "there are two independent tasks: A B" in {
        val testCase = new TestCase[EarlyStopTwoIndependentTasksOrchestrator](2, Set('A, 'B)) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationNumber(0)

              secondState.updatedExactStatuses(
                'A -> Finished,
                'B -> Waiting
              )
            }, { thirdState ⇒
              pingPongDestinationNumber(1)

              thirdState.updatedExactStatuses(
                'B -> Finished
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState ⇒ () }, // We don't want to test anything for this state
          { secondState ⇒ () }, // We don't want to test anything for this state
          { thirdState ⇒
            testStatus(orchestratorActor, statusProbe)(thirdState.expectedStatus)
            // Confirm we received the TerminatedEarly
            terminationProbe.expectMsg(TerminatedEarly)
            // Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()
          }, { fourthState ⇒
            // This confirms that when terminating early, tasks that are waiting will remain untouched
            // and the orchestrator will still be prepared to handle their responses
            testStatus(orchestratorActor, statusProbe)(fourthState.expectedStatus)

            // This confirms the method `onFinish` will NEVER be called even if the only tasks needed to finish
            // the orchestrator (aka Task B) are already waiting and its response is received.
            terminationProbe.expectNoMsg()
          }
        )
      }
      "there are two dependent tasks: A → B" in {

        val testCase = new TestCase[EarlyStopTwoLinearTasksOrchestrator](2, Set('A)) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationNumber(0)

              secondState.updatedExactStatuses(
                'A -> Finished
              )
            }
          )
        }

        import testCase._
        differentTestPerState(
          { firstState ⇒ () }, // We don't want to test anything for this state
          { secondState ⇒ () }, // We don't want to test anything for this state
          { thirdState ⇒
            testStatus(orchestratorActor, statusProbe)(thirdState.expectedStatus)
            // Confirm we received the TerminatedEarly
            terminationProbe.expectMsg(TerminatedEarly)
            // Confirm that onFinish was not invoked
            terminationProbe.expectNoMsg()

            // Confirm that every unstarted task (task B) will be prevented from starting even if its dependencies have finished.
            destinations(1).expectNoMsg()
          }
        )
      }
    }
  }
}
