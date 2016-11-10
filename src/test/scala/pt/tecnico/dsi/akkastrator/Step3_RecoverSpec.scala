package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.RecoverSpec._
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object RecoverSpec {
  class NoTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe)
  /**
    * A
    */
  class SingleTaskOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    echoFulltask("A", destinations(0))
  }
  /**
    * A
    * B
    * Both tasks have the same destination.
    */
  class TwoTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    echoFulltask("A", destinations(0))
    echoFulltask("B", destinations(0))
  }
  /**
    * A → B
    * Both tasks have the same destination.
    */
  class TwoLinearTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    echoFulltask("B", destinations(0), a :: HNil)
  }
  /**
    * A
    *  ⟩→ C
    * B
    * All tasks have distinct destinations.
    */
  class TasksInTOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    val b = echoFulltask("B", destinations(1))
    echoFulltask("C", destinations(2), a :: b :: HNil)
  }
  /**
    *     B
    *   ↗
    * A → C
    *   ↘
    *     D
    * All tasks have distinct destinations.
    */
  class FanOutTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    val b = echoFulltask("B", destinations(1), a :: HNil)
    val c = echoFulltask("C", destinations(2), a :: HNil)
    echoFulltask("D", destinations(3), a :: HNil)
  }
  /**
    *   B
    *  ↗ ↘
    * A → C
    * A and B have the same destination.
    */
  class TasksInTriangleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    val b = echoFulltask("B", destinations(0), a :: HNil)
    echoFulltask("C", destinations(1), a :: b :: HNil)
  }
  /**
    * A → C
    *   ↘  ⟩→ E
    * B → D
    * All tasks have distinct destinations.
    */
  class FiveTasksNoDepsBOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    val b = echoFulltask("B", destinations(1))
    val c = echoFulltask("C", destinations(2), a :: HNil)
    val d = echoFulltask("D", destinations(3), a :: b :: HNil)
    echoFulltask("E", destinations(4), c :: d :: HNil)
  }
  /**
    * A → B
    *   ↘  ⟩→ E
    * C → D
    * All tasks have distinct destinations.
    */
  class FiveTasksNoDepsCOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = echoFulltask("A", destinations(0))
    val b = echoFulltask("B", destinations(2), a :: HNil)
    val c = echoFulltask("C", destinations(1))
    val d = echoFulltask("D", destinations(3), a :: c :: HNil)
    echoFulltask("E", destinations(4), b :: d :: HNil)
  }
}
class RecoverSpec extends ActorSysSpec {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is recovered (actually computed), this is not directly tested
  //   if idsPerSender is not correctly recovered then the tasks will not recover to the correct state
  
  "A crashing orchestrator" should {
    "recover the correct state" when {
      "there is no tasks" in {
        val testCase0 = new TestCase[NoTasksOrchestrator](0, Set.empty) {
          val transformations = Seq.empty[State ⇒ State]
        }
        testCase0.testExpectedStatusWithRecovery()
      }
      "there is only a single task: A" in {
        val testCase1 = new TestCase[SingleTaskOrchestrator](1, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
              /**
                * In the transition from the 1st state to the 2nd state we send a StartOrchestrator to the orchestrator.
                * This will cause Task A to start and therefor to send a message to the destination(0).
                * Before task A receives the response we crash the orchestrator, which will cause it to restart and recover.
                * In the recover the task A will resend the message again since the delivery hasn't been confirmed yet.
                * So destination(0) gets a resend. And that is why we have 2 pingPongs here.
                */
              handleResend("A")
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              )
            }
          )
        }
        
        testCase1.testExpectedStatusWithRecovery()
      }
      """there are two independent tasks:
        |  A
        |  B""".stripMargin in {
        val testCase2 = new TestCase[TwoTasksOrchestrator](numberOfDestinations = 1, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
              //The message of B will arrive before of the resend of A, so we can't deal with it right away.
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              )
            }, { thirdState ⇒
              pingPong("B")
              handleResend("A")
              handleResend("B")
        
              thirdState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }
          )
        }
        testCase2.testExpectedStatusWithRecovery()
      }
      "there are two linear tasks: A → B" in {
        val testCase3 = new TestCase[TwoLinearTasksOrchestrator](numberOfDestinations = 1, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
              handleResend("A")
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPong("B")
              handleResend("B")
        
              thirdState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }
          )
        }
        testCase3.testExpectedStatusWithRecovery()
      }
      """there are three dependent tasks in T:
        |  A
        |   ⟩→ C
        |  B""".stripMargin in {
        val testCase4 = new TestCase[TasksInTOrchestrator](numberOfDestinations = 3, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("B")
        
              secondState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }, { thirdState ⇒
              pingPong("A")
              handleResend("B")
        
              thirdState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "C" -> Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              pingPong("C")
              handleResend("C")
              handleResend("A")
        
              fourthState.updatedExactStatuses(
                "C" -> Finished("finished")
              )
            }
          )
        }
        testCase4.testExpectedStatusWithRecovery()
      }
      """the tasks fan out:
        |     B
        |   ↗
        | A → C
        |   ↘
        |     D""".stripMargin in {
        val testCase5 = new TestCase[FanOutTasksOrchestrator](numberOfDestinations = 4, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
          
              secondState.updatedExactStatuses(
                "A" → Finished("finished")
              ).updatedStatuses(
                "B" → Set(Unstarted, Waiting),
                "C" → Set(Unstarted, Waiting),
                "D" → Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              handleResend("A")
              
              pingPong("B")
          
              thirdState.updatedExactStatuses(
                "B" → Finished("finished")
              ).updatedStatuses(
                "C" → Set(Waiting, Finished("finished")),
                "D" → Set(Waiting, Finished("finished"))
              )
            }, { fourthState ⇒
              pingPong("C")
              handleResend("B")
              handleResend("C")
              pingPong("D")
              handleResend("D")
          
              fourthState.updatedExactStatuses(
                "C" → Finished("finished"),
                "D" → Finished("finished")
              )
            }
          )
        }
        testCase5.testExpectedStatusWithRecovery()
      }
      """there are three dependent tasks in a triangle:
        |    B
        |   ↗ ↘
        |  A → C""".stripMargin in {
        val testCase6 = new TestCase[TasksInTriangleOrchestrator](numberOfDestinations = 2, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
              handleResend("A")
        
              secondState.updatedExactStatuses(
                "A" → Finished("finished")
              ).updatedStatuses(
                "B" → Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPong("B")
        
              thirdState.updatedExactStatuses(
                "B" → Finished("finished")
              ).updatedStatuses(
                "C" → Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              pingPong("C")
              handleResend("B")
              handleResend("C")
        
              fourthState.updatedExactStatuses(
                "C" → Finished("finished")
              )
            }
          )
        }
        testCase6.testExpectedStatusWithRecovery()
      }
      """there are five dependent tasks:
        |  A → C
        |    ↘  ⟩→ E
        |  B → D""".stripMargin in {
        val testCase7 = new TestCase[FiveTasksNoDepsBOrchestrator](numberOfDestinations = 5, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPong("C")
              handleResend("A")
        
              thirdState.updatedExactStatuses(
                "C" -> Finished("finished")
              )
            }, { fourthState ⇒
              pingPong("B")
        
              fourthState.updatedExactStatuses(
                "B" -> Finished("finished")
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fifthState ⇒
              pingPong("D")
              handleResend("B")
        
              fifthState.updatedExactStatuses(
                "D" -> Finished("finished")
              ).updatedStatuses(
                "E" -> Set(Unstarted, Waiting)
              )
            }, { sixthState ⇒
              pingPong("E")
              handleResend("C")
              handleResend("D")
              handleResend("E")
        
              sixthState.updatedExactStatuses(
                "E" -> Finished("finished")
              )
            }
          )
        }
        testCase7.testExpectedStatusWithRecovery()
      }
      """there are five dependent tasks:
        |  A → B
        |    ↘  ⟩→ E
        |  C → D""".stripMargin in {
        val testCase8 = new TestCase[FiveTasksNoDepsCOrchestrator](numberOfDestinations = 5, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPong("A")
          
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPong("C")
          
              thirdState.updatedExactStatuses(
                "C" -> Finished("finished")
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              pingPong("B")
              handleResend("A")
          
              fourthState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }, { fifthState ⇒
              pingPong("D")
              handleResend("C")
          
              fifthState.updatedExactStatuses(
                "D" -> Finished("finished")
              ).updatedStatuses(
                "E" -> Set(Unstarted, Waiting)
              )
            }, { sixthState ⇒
              pingPong("E")
              handleResend("B")
              handleResend("D")
              handleResend("E")
          
              sixthState.updatedExactStatuses(
                "E" -> Finished("finished")
              )
            }
          )
        }
        testCase8.testExpectedStatusWithRecovery()
      }
    }
  }
}