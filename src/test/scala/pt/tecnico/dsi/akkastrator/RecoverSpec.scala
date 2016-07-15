package pt.tecnico.dsi.akkastrator

class RecoverSpec extends ActorSysSpec {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is recovered (actually computed), this is not directly tested
  //   if idsPerSender is not correctly recovered then the tasks will not recover to the correct state

  def testOrchestrator(testCase: TestCase[_]): Unit = {
    import testCase._
  
    sameTestPerState { state ⇒
      // First we test the orchestrator is in the expected state (aka the status is what we expect)
      testStatus(orchestratorActor, statusProbe)(state.expectedStatus)
      // Then we crash the orchestrator
      orchestratorActor ! "boom"
      // Finally we test that the orchestrator recovered to the expected state
      testStatus(orchestratorActor, statusProbe)(state.expectedStatus)
    }
  }

  "A crashing orchestrator" should {
    "recover the correct state" when {
      /*
      """there is only a single task:
        |  A""".stripMargin in {
        testOrchestrator(testCase1)
      }
      */
      """there are two independent tasks:
        |  A
        |  B""".stripMargin in {
        testOrchestrator(testCase2)
      }
      /*
      """there are two linear tasks:
        |  A → B""".stripMargin in {
        testOrchestrator(testCase3)
      }
      */
      /*
      """there are three dependent tasks in T:
        |  A
        |   ⟩→ C
        |  B""".stripMargin in {
        testOrchestrator(testCase4)
      }
      """there are three dependent tasks in a triangle:
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
      */
    }
  }
}
