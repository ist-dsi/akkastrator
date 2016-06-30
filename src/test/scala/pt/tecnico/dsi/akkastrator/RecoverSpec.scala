package pt.tecnico.dsi.akkastrator

import org.scalatest.FunSuiteLike

import scala.concurrent.duration.DurationInt

class RecoverSpec extends ActorSysSpec with FunSuiteLike with TestCases {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is recovered (actually computed), this is not directly tested
  //   if idsPerSender is not correctly recovered then the tasks will not recover to the correct state

  def testOrchestrator(testCase: TestCase[_]): Unit = {
    import testCase._
    testForEachState { case (probe, state) ⇒
      logger.info(s"State: ${state.expectedStatusSeq}")

      testStatus(orchestratorActor, probe, maxDuration = 1.second)(state.expectedStatusSeq:_*)
      orchestratorActor ! "boom"
      testStatus(orchestratorActor, probe, maxDuration = 1.second)(state.expectedStatusSeq:_*)
    }
  }

  test("""Case 1:
         |  A""".stripMargin) {
    testOrchestrator(testCase1)
  }

  test("""Case 2:
         |  A
         |  B""".stripMargin) {
    testOrchestrator(testCase2)
  }

  test("""Case 3:
         |  A — B""".stripMargin) {
    testOrchestrator(testCase3)
  }

  test("""Case 4:
         |    A
         |     \
         |  B — C""".stripMargin) {
    testOrchestrator(testCase4)
  }

  test("""Case 5:
         |    B
         |   / \
         |  A — C""".stripMargin) {
    testOrchestrator(testCase5)
  }

  test("""Case 6:
         |  A — B
         |    \  ⟩- C
         |  C — D""".stripMargin) {
    testOrchestrator(testCase6)
  }
}
