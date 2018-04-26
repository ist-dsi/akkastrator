package pt.tecnico.dsi.akkastrator

import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.Step3_RecoverSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HNil

object Step3_RecoverSpec {
  class NoTasks(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations)
  /** A */
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
  /** A → B */
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
  // Ensure that when the orchestrator crashes
  //  · the correct state of the tasks is recovered
  //  · the correct idsPerSender is re-computed. If its not the tasks will not recover to the correct state
  
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
        val testCase1 = new TestCase[SingleTask](1, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished("finished")
              )
            }
          )
        }
        testCase1.testExpectedStatusWithRecovery()
      }
      """there are two independent tasks:
        |  A
        |  B""".stripMargin in {
        val testCase2 = new TestCase[TwoTasks](numberOfDestinations = 1, Set(0, 1)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
              pingPong(destinations(0)) // Destination of Task "B"

              secondState.updatedStatuses(
                0 -> Finished("finished"),
                1 -> Finished("finished")
              )
            }
          )
        }
        testCase2.testExpectedStatusWithRecovery()
      }
      "there are two linear tasks: A → B" in {
        val testCase3 = new TestCase[TwoLinearTasks](numberOfDestinations = 2, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished("finished"),
                1 -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong(destinations(1)) // Destination of Task "B"

              thirdState.updatedStatuses(
                0 -> Finished("finished")
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
        val testCase4 = new TestCase[TasksInT](numberOfDestinations = 3, Set(0, 1)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(1)) // Destination of Task "B"
        
              secondState.updatedStatuses(
                1 -> Finished("finished")
              )
            }, { thirdState =>
              pingPong(destinations(0)) // Destination of Task "A"

              thirdState.updatedStatuses(
                0 -> Finished("finished"),
                2 -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong(destinations(2)) // Destination of Task "C"

              fourthState.updatedStatuses(
                2 -> Finished("finished")
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
        val testCase5 = new TestCase[FanOutTasks](numberOfDestinations = 4, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
          
              secondState.updatedStatuses(
                0 -> Finished("finished"),
                1 -> Unstarted or Waiting,
                2 -> Unstarted or Waiting,
                3 -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong(destinations(1)) // Destination of Task "B"
          
              thirdState.updatedStatuses(
                1 -> Finished("finished"),
                2 -> Waiting or Finished("finished"),
                3 -> Waiting or Finished("finished")
              )
            }, { fourthState =>
              pingPong(destinations(2)) // Destination of Task "C"
              pingPong(destinations(3)) // Destination of Task "D"

              fourthState.updatedStatuses(
                2 -> Finished("finished"),
                3 -> Finished("finished")
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
        val testCase6 = new TestCase[TasksInTriangle](numberOfDestinations = 3, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished("finished"),
                1 -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong(destinations(1)) // Destination of Task "B"

              thirdState.updatedStatuses(
                1 -> Finished("finished"),
                2 -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong(destinations(2)) // Destination of Task "C"

              fourthState.updatedStatuses(
                2 -> Finished("finished")
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
        val testCase7 = new TestCase[FiveTasksNoDepsB](numberOfDestinations = 5, Set(0, 1)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished("finished"),
                2 -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong(destinations(2)) // Destination of Task "C"

              thirdState.updatedStatuses(
                2 -> Finished("finished")
              )
            }, { fourthState =>
              pingPong(destinations(1)) // Destination of Task "B"

              fourthState.updatedStatuses(
                1 -> Finished("finished"),
                3 -> Unstarted or Waiting
              )
            }, { fifthState =>
              pingPong(destinations(3)) // Destination of Task "D"

              fifthState.updatedStatuses(
                3 -> Finished("finished"),
                4 -> Unstarted or Waiting
              )
            }, { sixthState =>
              pingPong(destinations(4)) // Destination of Task "E"

              sixthState.updatedStatuses(
                4 -> Finished("finished")
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
        val testCase8 = new TestCase[FiveTasksNoDepsC](numberOfDestinations = 5, Set(0, 1)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished("finished"),
                1 -> Unstarted or Waiting
              )
            }, { thirdState =>
              pingPong(destinations(1)) // Destination of Task "C"

              thirdState.updatedStatuses(
                2 -> Finished("finished"),
                3 -> Unstarted or Waiting
              )
            }, { fourthState =>
              pingPong(destinations(2)) // Destination of Task "B"

              fourthState.updatedStatuses(
                1 -> Finished("finished")
              )
            }, { fifthState =>
              pingPong(destinations(3)) // Destination of Task "D"

              fifthState.updatedStatuses(
                3 -> Finished("finished"),
                4 -> Unstarted or Waiting
              )
            }, { sixthState =>
              pingPong(destinations(4)) // Destination of Task "E"

              sixthState.updatedStatuses(
                4 -> Finished("finished")
              )
            }
          )
        }
        testCase8.testExpectedStatusWithRecovery()
      }
    }
  }
}