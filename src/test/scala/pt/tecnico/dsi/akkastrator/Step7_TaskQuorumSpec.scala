package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration
import scala.util.Random

import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL._
import pt.tecnico.dsi.akkastrator.Step7_TaskQuorumSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.{::, HNil}

object Step7_TaskQuorumSpec {
  class TasksWithSameDestinationQuorum(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    // A - error: tasks with same destination
    FullTask("A") createTaskWith { case HNil =>
      new TaskQuorum(_)(o => Seq(
        simpleMessageFulltask("0", destinations(0), "0")(o),
        simpleMessageFulltask("1", destinations(0), "1")(o)
      ))
    }
  }
  class TasksWithDifferentMessagesQuorum(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    case class AnotherMessage(s: String, id: Long)
    
    FullTask("A") createTaskWith { case HNil =>
      new TaskQuorum(_)(o => Seq(
        simpleMessageFulltask("0", destinations(0), "0")(o),
        fulltask("1", destinations(1), AnotherMessage("1", _), "1")(o)
      ))
    }
  }
  
  // The length of each string is important. Do not change them. See SimpleTaskQuorumOrchestrator below.
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maraca")
  class SimpleTaskQuorum(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    // A -> N*B
    val a = simpleMessageFulltask("A", destinations(0), aResult)
    FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskQuorum(_)(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          fulltask(s"$fruit-B", destinations(i + 1), SimpleMessage("B-InnerTask", _), fruit.length)(o)
        }
      )
    }
  }
  class ComplexTaskQuorum(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    //     N*B
    // A →⟨   ⟩→ 2*N*D
    //     N*C
    val a = simpleMessageFulltask("A", destinations(0), aResult)
    val b = FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskQuorum(_)(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          fulltask(s"B-$fruit", destinations(i + 6), SimpleMessage("B message", _), fruit.length)(o)
        }
      )
    }
    val c = FullTask("C", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskQuorum(_, AtLeast(2))(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          fulltask(s"C-$fruit", destinations(i + 1), SimpleMessage("C message", _), fruit.length)(o)
        }
      )
    }
    // Using tuple syntax makes it prettier
    FullTask("D", (b, c), Duration.Inf) createTask { case (fruitsLengthB, fruitsLengthC) =>
      new TaskQuorum(_, All)(o => Seq(
        fulltask("D-B", destinations(11), SimpleMessage("D message", _), fruitsLengthB)(o),
        fulltask("D-C", destinations(12), SimpleMessage("D message", _), fruitsLengthC)(o)
      ))
    }
  }
  
  class SurpassingTolerance(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    FullTask("A") createTaskWith { case HNil =>
      // Since minimumVotes = All the tolerance is 0.
      // Given that one of the tasks aborts so should the Quorum
      new TaskQuorum(_, minimumVotes = All)(o =>
        Seq(
          fulltask(s"0-B", destinations(0), SimpleMessage("B-InnerTask", _), 5)(o),
          fulltask(s"1-B", destinations(1), SimpleMessage("B-InnerTask", _), 5)(o),
          fulltask(s"2-B", destinations(2), SimpleMessage("B-InnerTask", _), 5, abortOnReceive = true)(o)
        )
      )
    }
  }
  class NotReachingTolerance(destinations: Array[TestProbe]) extends ControllableOrchestrator() {
    FullTask("A") createTaskWith { case HNil =>
      // Since minimumVotes = Majority (the default) and there are 5 tasks the tolerance is 2.
      // Given that only one task aborts the Quorum should succeed.
      new TaskQuorum(_, minimumVotes = All)(o =>
        Seq(
          fulltask(s"0-B", destinations(0), SimpleMessage("B-InnerTask", _), 5)(o),
          fulltask(s"1-B", destinations(1), SimpleMessage("B-InnerTask", _), 5)(o),
          fulltask(s"2-B", destinations(2), SimpleMessage("B-InnerTask", _), 5, abortOnReceive = true)(o),
          fulltask(s"3-B", destinations(2), SimpleMessage("B-InnerTask", _), 3)(o),
          fulltask(s"4-B", destinations(2), SimpleMessage("B-InnerTask", _), 5)(o)
        )
      )
    }
  }
}
class Step7_TaskQuorumSpec extends ActorSysSpec {
  "An orchestrator with task quorum" should {
    "fail" when {
      "the tasksCreator generates tasks with the same destination" in {
        val testCase = new TestCase[TasksWithSameDestinationQuorum](1, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              parentProbe expectMsg OrchestratorAborted
              
              secondState.updatedStatuses(
                "A" -> Aborted(new IllegalArgumentException("TasksCreator must generate tasks with distinct destinations."))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "the tasksCreator generates tasks with different messages" in {
        val testCase = new TestCase[TasksWithDifferentMessagesQuorum](2, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              parentProbe expectMsg OrchestratorAborted
          
              secondState.updatedStatuses(
                "A" -> Aborted(new IllegalArgumentException("TasksCreator must generate tasks with the same message."))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    "behave according to the documentation" when {
      "there is only a single quorum: A -> N*B - one task doesn't answer" in {
        val testCase = new TestCase[SimpleTaskQuorum](numberOfDestinations = 6, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
              
              secondState.updatedStatuses(
                "A" -> Finished(aResult)
              )
            }, { thirdState =>
              // One of the tasks in the quorum wont give out an answer
              Random.shuffle(1 to 5).toSeq.drop(1).par.foreach { i =>
                pingPong(destinations(i))
              }
              
              handleResend("A")
              
              // However because B is a quorum it should terminate
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedStatuses(
                "B" -> Set(Waiting, Finished(6))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      
      """there are a complex web of quorums:
        |     N*B
        | A →⟨   ⟩→ 2*N*D
        |     N*C
      """.stripMargin in {
        val testCase = new TestCase[ComplexTaskQuorum](numberOfDestinations = 13, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
              
              secondState.updatedStatuses(
                "A" -> Finished(aResult),
                "B" -> Unstarted or Waiting,
                "C" -> Unstarted or Waiting
              )
            }, { thirdState =>
              // For B tasks
              Random.shuffle(1 to 5).drop(1).foreach { i =>
                pingPong(destinations(i + 5))
              }
              // For C tasks
              Random.shuffle(1 to 5).drop(1).foreach { i =>
                pingPong(destinations(i))
              }
                            
              handleResend("A")
              
              expectInnerOrchestratorTermination("C")
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedStatuses(
                "B" -> Finished(6),
                "C" -> Finished(6),
                "D" -> Unstarted or Waiting
              )
            }, { fourthState =>
              // D Tasks
              pingPong(destinations(6))
              pingPong(destinations(7))
              
              import scala.concurrent.duration.DurationInt
              expectInnerOrchestratorTermination("D", 20.seconds)
              
              fourthState.updatedStatuses(
                "D" -> Finished(6)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
  
    //TODO: test aborts, tolerance is surpassed
    //TODO: test aborts, tolerance is not reached
    //TODO: test QuorumNotAchieved
    
    //TODO: test timeouts
  }
}
