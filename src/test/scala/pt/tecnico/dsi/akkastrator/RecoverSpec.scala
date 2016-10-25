package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.RecoverSpec._
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import pt.tecnico.dsi.akkastrator.Work._

object RecoverSpec {
  class SingleTaskOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    echoTask("A", destinations(0).ref.path)
  }
  class TwoIndependentTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    // B
    echoTask("A", destinations(0).ref.path)
    echoTask("B", destinations(0).ref.path)
  }
  class TwoLinearTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → B
    val a = echoTask("A", destinations(0).ref.path)
    echoTask("B", destinations(0).ref.path, dependencies = Set(a))
  }
  class TasksInTOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A
    //  ⟩→ C
    // B
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(1).ref.path)
    echoTask("C", destinations(2).ref.path, dependencies = Set(a, b))
  }
  class TasksInTriangleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //   B
    //  ↗ ↘
    // A → C
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(0).ref.path, dependencies = Set(a))
    echoTask("C", destinations(1).ref.path, dependencies = Set(a, b))
  }
  class FiveTasksOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A → C
    //   ↘  ⟩→ E
    // B → D
    val a = echoTask("A", destinations(0).ref.path)
    val b = echoTask("B", destinations(1).ref.path)
    val c = echoTask("C", destinations(2).ref.path, dependencies = Set(a))
    val d = echoTask("D", destinations(3).ref.path, dependencies = Set(a, b))
    echoTask("E", destinations(4).ref.path, dependencies = Set(c, d))
  }
}
class RecoverSpec extends ActorSysSpec {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is recovered (actually computed), this is not directly tested
  //   if idsPerSender is not correctly recovered then the tasks will not recover to the correct state
  
  "A crashing orchestrator" should {
    "recover the correct state" when {
      "there is only a single task: A" in {
        /**
          * Destinations:
          *  A -> destination(0)
          *
          * Test points:
          *  · Before any task starts
          *  · After task A starts
          *  · After Task A finishes
          */
        val testCase1 = new TestCase[SingleTaskOrchestrator](1, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("A")
              /**
                * In the transition from the 1st state to the 2nd state we send a StartOrchestrator to the orchestrator.
                * This will cause Task A to start and therefor to send a message to the destination(0).
                * Before task A receives the response we crash the orchestrator, which will cause it to restart and recover.
                * In the recover the task A will resend the message again since the delivery hasn't been confirmed yet.
                * So destination(0) gets a resend. And that is why we have 2 pingPongs here.
                */
              pingPongTestProbeOf("A")
        
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
        /**
          * Destinations:
          *  A -> destination(0)
          *  B -> destination(0)
          *
          * Test points:
          *  · Before any task starts
          *  · After both tasks start
          *  · After task A finishes
          *  · After task B finishes
          */
        val testCase2 = new TestCase[TwoIndependentTasksOrchestrator](1, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("A")
              //The message of B will arrive before of the resend of A, so we can't deal with it right away.
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              )
            }, { thirdState ⇒
              pingPongTestProbeOf("B")
              //Resend of A
              pingPongTestProbeOf("A")
              //Resend of B
              pingPongTestProbeOf("B")
        
              thirdState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }
          )
        }
        testCase2.testExpectedStatusWithRecovery()
      }
      "there are two linear tasks: A → B" in {
        /**
          * Destinations:
          *  A -> destination(0)
          *  B -> destination(0)
          *
          * Test points:
          *  · Before any task starts
          *  · After Task A starts
          *  · After Task A finishes and Task B is about to start or has already started
          *  · After Task B finishes
          */
        val testCase3 = new TestCase[TwoLinearTasksOrchestrator](1, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("A")
              //Resend of A
              pingPongTestProbeOf("A")
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPongTestProbeOf("B")
              //Resend of B
              pingPongTestProbeOf("B")
        
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
        /**
          * Destinations:
          *  A -> destination(0)
          *  B -> destination(1)
          *  C -> destination(2)
          *
          * Test points:
          *  · Before any task starts
          *  · After Task A and Task B start
          *  · After Task B finishes
          *  · After Task B finishes
          */
        val testCase4 = new TestCase[TasksInTOrchestrator](3, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("B")
        
              secondState.updatedExactStatuses(
                "B" -> Finished("finished")
              )
            }, { thirdState ⇒
              pingPongTestProbeOf("A")
              //Resend of B. We do it here because it also must work here.
              pingPongTestProbeOf("B")
        
              thirdState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "C" -> Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              pingPongTestProbeOf("C")
              //Resend of C
              pingPongTestProbeOf("C")
              //Resend of A. We do it here because it also must work here.
              pingPongTestProbeOf("A")
        
              fourthState.updatedExactStatuses(
                "C" -> Finished("finished")
              )
            }
          )
        }
        testCase4.testExpectedStatusWithRecovery()
      }
      """there are three dependent tasks in a triangle:
        |    B
        |   ↗ ↘
        |  A → C""".stripMargin in {
        /**
          * Destinations:
          *  A -> destination(0)
          *  B -> destination(0)
          *  C -> destination(1)
          *
          * Test points:
          *  · Before any task starts
          *  · After Task A starts
          *  · After Task A finishes and Task B is about to start or has already started
          *  · After Task B finishes and Task C is about to start or has already started
          *  · After Task C finishes
          */
        val testCase5 = new TestCase[TasksInTriangleOrchestrator](2, Set("A")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("A")
              //Resend of A
              pingPongTestProbeOf("A")
        
              secondState.updatedExactStatuses(
                "A" → Finished("finished")
              ).updatedStatuses(
                "B" → Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPongTestProbeOf("B")
        
              thirdState.updatedExactStatuses(
                "B" → Finished("finished")
              ).updatedStatuses(
                "C" → Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              pingPongTestProbeOf("C")
              //Resend of B
              pingPongTestProbeOf("B")
              //Resend of C
              pingPongTestProbeOf("C")
        
              fourthState.updatedExactStatuses(
                "C" → Finished("finished")
              )
            }
          )
        }
        testCase5.testExpectedStatusWithRecovery()
      }
      """there are five dependent tasks:
        |  A → C
        |    ↘  ⟩→ E
        |  B → D""".stripMargin in {
        /**
          * Destinations:
          *  A -> destination(0)
          *  B -> destination(1)
          *  C -> destination(2)
          *  D -> destination(3)
          *  E -> destination(4)
          *
          *  Test points:
          *   · Before any task starts
          *   · After Task A and B start
          *   · After Task A finishes and Task C is about to start or has already started
          *   · After Task C finishes
          *   · After Task B finishes and Task D is about to start or has already started
          *   · After Task D finishes and Task E is about to start or has already started
          *   · After Task E finishes
          */
        val testCase6 = new TestCase[FiveTasksOrchestrator](5, Set("A", "B")) {
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongTestProbeOf("A")
        
              secondState.updatedExactStatuses(
                "A" -> Finished("finished")
              ).updatedStatuses(
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              pingPongTestProbeOf("C")
        
              thirdState.updatedExactStatuses(
                "C" -> Finished("finished")
              )
            }, { fourthState ⇒
              pingPongTestProbeOf("B")
        
              fourthState.updatedExactStatuses(
                "B" -> Finished("finished")
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fifthState ⇒
              pingPongTestProbeOf("D")
        
              fifthState.updatedExactStatuses(
                "D" -> Finished("finished")
              ).updatedStatuses(
                "E" -> Set(Unstarted, Waiting)
              )
            }, { sixthState ⇒
              pingPongTestProbeOf("E")
        
              sixthState.updatedExactStatuses(
                "E" -> Finished("finished")
              )
            }
          )
        }
        testCase6.testExpectedStatusWithRecovery()
      }
    }
  }
}
