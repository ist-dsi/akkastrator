package pt.tecnico.dsi.akkastrator

import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.Step3_RecoverSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object Step3_RecoverSpec {
  class NoTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations)
  /**
    * A
    */
  class SingleTask(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    simpleMessageFulltask("A", 0)
  }
  /**
    * A
    * B
    * Both tasks have the same destination.
    */
  class TwoTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    simpleMessageFulltask("A", 0)
    simpleMessageFulltask("B", 0)
  }
  /**
    * A → B
    * Both tasks have the same destination.
    */
  class TwoLinearTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    simpleMessageFulltask("B", 1, dependencies = a :: HNil)
  }
  /**
    * A
    *  ⟩→ C
    * B
    * All tasks have distinct destinations.
    */
  class TasksInT(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    val b = simpleMessageFulltask("B", 1)
    simpleMessageFulltask("C", 2, dependencies = a :: b :: HNil)
  }
  /**
    *     B
    *   ↗
    * A → C
    *   ↘
    *     D
    * All tasks have distinct destinations.
    */
  class FanOutTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    val b = simpleMessageFulltask("B", 1, dependencies = a :: HNil)
    val c = simpleMessageFulltask("C", 2, dependencies = a :: HNil)
    simpleMessageFulltask("D", 3, dependencies = a :: HNil)
  }
  /**
    *   B
    *  ↗ ↘
    * A → C
    * A and B have the same destination.
    */
  class TasksInTriangle(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    val b = simpleMessageFulltask("B", 1, dependencies = a :: HNil)
    simpleMessageFulltask("C", 2, dependencies = a :: b :: HNil)
  }
  /**
    * A → C
    *   ↘  ⟩→ E
    * B → D
    * All tasks have distinct destinations.
    */
  class FiveTasksNoDepsB(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    val b = simpleMessageFulltask("B", 1)
    val c = simpleMessageFulltask("C", 2, dependencies = a :: HNil)
    val d = simpleMessageFulltask("D", 3, dependencies = a :: b :: HNil)
    simpleMessageFulltask("E", 4, dependencies = c :: d :: HNil)
  }
  /**
    * A → B
    *   ↘  ⟩→ E
    * C → D
    * All tasks have distinct destinations.
    */
  class FiveTasksNoDepsC(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0)
    val b = simpleMessageFulltask("B", 2, dependencies = a :: HNil)
    val c = simpleMessageFulltask("C", 1)
    val d = simpleMessageFulltask("D", 3, dependencies = a :: c :: HNil)
    simpleMessageFulltask("E", 4, dependencies = b :: d :: HNil)
  }
}
class Step3_RecoverSpec extends ActorSysSpec {
  //Ensure that when the orchestrator crashes
  // · the correct state of the tasks is recovered
  // · the correct idsPerSender is re-computed. If its not the tasks will not recover to the correct state
  
  "A crashing orchestrator" should {
    "recover the correct state" when {
      "there is no tasks" in {
        val testCase0 = new TestCase[NoTasks](0, Set.empty) {
          // Purposefully left without any additional transformation
          val transformations = withStartAndFinishTransformations()
        }
        testCase0.testExpectedStatusWithRecovery()
      }
      "there is only a single task: A" in {
        val testCase1 = new TestCase[SingleTask](1, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
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
        val testCase2 = new TestCase[TwoTasks](numberOfDestinations = 1, Set("A", "B")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
              pingPong("B")
    
              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "B" -> Finished("finished")
              )
            }
          )
        }
        testCase2.testExpectedStatusWithRecovery()
      }
      "there are two linear tasks: A → B" in {
        val testCase3 = new TestCase[TwoLinearTasks](numberOfDestinations = 2, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "B" -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong("B")

              thirdState.updatedStatuses(
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
        val testCase4 = new TestCase[TasksInT](numberOfDestinations = 3, Set("A", "B")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("B")
        
              secondState.updatedStatuses(
                "B" -> Finished("finished")
              )
            }, { thirdState =>
              pingPong("A")

              thirdState.updatedStatuses(
                "A" -> Finished("finished"),
                "C" -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong("C")

              fourthState.updatedStatuses(
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
        val testCase5 = new TestCase[FanOutTasks](numberOfDestinations = 4, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
          
              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "B" -> Unstarted or Waiting,
                "C" -> Unstarted or Waiting,
                "D" -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong("B")
          
              thirdState.updatedStatuses(
                "B" -> Finished("finished"),
                "C" -> Waiting or Finished("finished"),
                "D" -> Waiting or Finished("finished")
              )
            }, { fourthState =>
              pingPong("C")
              pingPong("D")

              fourthState.updatedStatuses(
                "C" -> Finished("finished"),
                "D" -> Finished("finished")
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
        val testCase6 = new TestCase[TasksInTriangle](numberOfDestinations = 3, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")

              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "B" -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong("B")
        
              thirdState.updatedStatuses(
                "B" -> Finished("finished"),
                "C" -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong("C")

              fourthState.updatedStatuses(
                "C" -> Finished("finished")
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
        val testCase7 = new TestCase[FiveTasksNoDepsB](numberOfDestinations = 5, Set("A", "B")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
        
              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "C" -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong("C")

              thirdState.updatedStatuses(
                "C" -> Finished("finished")
              )
            }, { fourthState =>
              pingPong("B")
        
              fourthState.updatedStatuses(
                "B" -> Finished("finished"),
                "D" -> Unstarted or Waiting
              )
            }, { fifthState =>
              pingPong("D")

              fifthState.updatedStatuses(
                "D" -> Finished("finished"),
                "E" -> Unstarted or Waiting
              )
            }, { sixthState =>
              pingPong("E")

              sixthState.updatedStatuses(
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
        val testCase8 = new TestCase[FiveTasksNoDepsC](numberOfDestinations = 5, Set("A", "B")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
          
              secondState.updatedStatuses(
                "A" -> Finished("finished"),
                "B" -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong("C")
          
              thirdState.updatedStatuses(
                "C" -> Finished("finished"),
                "D" -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong("B")

              fourthState.updatedStatuses(
                "B" -> Finished("finished")
              )
            }, { fifthState =>
              pingPong("D")

              fifthState.updatedStatuses(
                "D" -> Finished("finished"),
                "E" -> Unstarted or Waiting
              )
            }, { sixthState =>
              pingPong("E")

              sixthState.updatedStatuses(
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