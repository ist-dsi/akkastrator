package pt.tecnico.dsi.akkastrator

import org.scalatest.WordSpecLike

class RecoverSpec extends ActorSysSpec with WordSpecLike with TestCases {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is recovered (actually computed), this is not directly tested
  //   if idsPerSender is not correctly recovered then the tasks will not recover to the correct state

  def testOrchestrator(testCase: TestCase[_]): Unit = {
    import testCase._
    testForEachState { case (probe, state) ⇒
      testStatus(orchestratorActor, probe)(state.expectedStatus)
      orchestratorActor ! "boom"
      testStatus(orchestratorActor, probe)(state.expectedStatus)
    }
  }

  "A crashing orchestrator" should {
    "recover the correct state" when {
      """there is only a single task:
        |  A""".stripMargin in {
        testOrchestrator(testCase1)
      }
      """there are two independent tasks:
        |  A
        |  B""".stripMargin in {
        testOrchestrator(testCase2)
      }
      """there are two dependent tasks:
        |  A → B""".stripMargin in {
        testOrchestrator(testCase3)
      }
      """there are three dependent tasks:
        |  A
        |   ⟩→ C
        |  B""".stripMargin in {
        testOrchestrator(testCase4)
      }
      """there are three dependent tasks:
        |    B
        |   ↗ ↘
        |  A → C""".stripMargin in {
        testOrchestrator(testCase5)
      }
      """there are five dependent tasks:
        |  A → B
        |    ↘  ⟩→ C
        |  C → D""".stripMargin in {
        testOrchestrator(testCase6)
      }
    }
  }
}
