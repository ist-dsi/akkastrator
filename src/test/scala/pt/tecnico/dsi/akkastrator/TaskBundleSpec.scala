package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import pt.tecnico.dsi.akkastrator.TaskBundleSpec.SimpleTaskBundleOrchestrator

object TaskBundleSpec {
  class SimpleTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> N*B
    val a = task("A", destinations(0).ref.path, 1 to 5)
    val b = taskBundle(a.result.get, destinations(1).ref.path, "B", Set(a))
  }
}

class TaskBundleSpec extends ActorSysSpec {
  "An orchestrator with task bundles" should {
    "behave according to the documentation" when {
      "there is only a single task bundler: A -> N*B" in {
        val testCase = new TestCase[SimpleTaskBundleOrchestrator](2, Set('A)) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationOf('A)
              
              secondState.updatedExactStatuses(
                'A → Finished(1 to 5)
              )
            }, { thirdState ⇒
              for(_ ← 1 to 5) {
                val m = destinations(1).expectMsgClass(classOf[SimpleMessage])
                destinations(1).reply(m)
              }
              
              //Give some time for the inner orchestrator tasks to finish
              //FIXME: remove the sleep
              Thread.sleep(100)
              
              thirdState.updatedStatuses(
                'B → Set(Waiting(2L), Finished(Seq.fill(5)("finished")))
              )
            }
          )
        }
        import testCase._
        sameTestPerState { state ⇒
          // First we test the orchestrator is in the expected state (aka the status is what we expect)
          testStatus(state.expectedStatus)
          // Then we crash the orchestrator
          orchestratorActor ! "boom"
          // Finally we test that the orchestrator recovered to the expected state
          testStatus(state.expectedStatus)
        }
      }
    }
  }
}
