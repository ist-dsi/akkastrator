package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration
import scala.util.Random

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, OrchestratorAborted}
import pt.tecnico.dsi.akkastrator.DSL._
import pt.tecnico.dsi.akkastrator.Step6_TaskQuorumSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.{::, HNil}

object Step6_TaskQuorumSpec {
  // The length of each string is important. Do not change them. See SimpleTaskQuorumOrchestrator below.
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maraca")
  
  class TasksWithSameDestinationQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A - error: tasks with same destination
    FullTask("A") createTaskWith { case HNil =>
      new TaskQuorum(_)(o => Seq(
        simpleMessagefulltask("0", destinations(0), "0")(o),
        simpleMessagefulltask("1", destinations(0), "1")(o)
      ))
    }
  }
  class TasksWithDifferentMessagesQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    case class AnotherMessage(s: String, id: Long)
    
    FullTask("A") createTaskWith { case HNil =>
      new TaskQuorum(_)(o => Seq(
        simpleMessagefulltask("0", destinations(0), "0")(o),
        fulltask("1", destinations(1), AnotherMessage("1", _), "1")(o)
      ))
    }
  }
  
  class SimpleTaskQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> N*B
    val a = simpleMessagefulltask("A", destinations(0), aResult)
    FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskQuorum(_)(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          fulltask(s"$fruit-B", destinations(i + 1), SimpleMessage("B-InnerTask", _), fruit.length)(o)
        }
      )
    }
  }
  class ComplexTaskQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //     N*B
    // A →⟨   ⟩→ 2*N*D
    //     N*C
    val a = simpleMessagefulltask("A", destinations(0), aResult)
    val b = FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskQuorum(_)(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          fulltask(s"B-$fruit", destinations(i + 1), SimpleMessage("B message", _), fruit.length)(o)
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
    FullTask("D", (b, c), Duration.Inf) createTaskWith { case fruitsLengthB :: fruitsLengthC :: HNil =>
      new TaskQuorum(_)(o =>
        Seq(fruitsLengthB, fruitsLengthC).zipWithIndex.map { case (fruit, i) =>
          fulltask(s"D-$i", destinations(i + 1), SimpleMessage(fruit.toString, _), fruit)(o)
        }
      )
    }
  }
}
class Step6_TaskQuorumSpec extends ActorSysSpec {
  "An orchestrator with task quorum" should {
    "must fail" when {
      "the tasksCreator generates tasks with the same destination" in {
        val testCase = new TestCase[TasksWithSameDestinationQuorumOrchestrator](1, Set("A")) {
          val transformations: Seq[(State) => State] = Seq(
            { secondState =>
              terminationProbe.expectMsg(OrchestratorAborted)
              
              secondState.updatedExactStatuses(
                "A" -> Aborted(InitializationError("TasksCreator must generate tasks with distinct destinations."))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "the tasksCreator generates tasks with different messages" in {
        val testCase = new TestCase[TasksWithDifferentMessagesQuorumOrchestrator](2, Set("A")) {
          val transformations: Seq[(State) => State] = Seq(
            { secondState =>
              terminationProbe.expectMsg(OrchestratorAborted)
          
              secondState.updatedExactStatuses(
                "A" -> Aborted(InitializationError("TasksCreator must generate tasks with the same message."))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    "behave according to the documentation" when {
      "there is only a single quorum: A -> N*B - one task doesn't answer" in {
        val testCase = new TestCase[SimpleTaskQuorumOrchestrator](numberOfDestinations = 6, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              )
            }, { thirdState =>
              // One of the tasks in the quorum wont give out an answer
              Random.shuffle(1 to 5).toSeq.drop(1).par.foreach { i =>
                pingPong(destinations(i))
              }
              
              handleResend("A")
              
              // However because B is a quorum is should terminate
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
        val testCase = new TestCase[ComplexTaskQuorumOrchestrator](6, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting),
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              Random.shuffle(1 to 5).toSeq.drop(1).par.foreach { i =>
                pingPong(destinations(i)) // For B tasks
              }
              Random.shuffle(1 to 5).toSeq.drop(1).par.foreach { i =>
                pingPong(destinations(i)) // For C tasks
              }
                            
              handleResend("A")
              
              expectInnerOrchestratorTermination("B")
              expectInnerOrchestratorTermination("C")
              
              thirdState.updatedStatuses(
                "B" -> Set(Waiting, Finished(6)),
                "C" -> Set(Waiting, Finished(6)),
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fourthState =>
              // D Tasks
              pingPong(destinations(1))
              pingPong(destinations(2))
              
              import scala.concurrent.duration.DurationInt
              expectInnerOrchestratorTermination("D", 10.seconds)
              
              fourthState.updatedExactStatuses(
                "B" -> Finished(6),
                "C" -> Finished(6)
              ).updatedStatuses(
                "D" -> Set(Waiting, Finished(6))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    
    //TODO: test aborts, more specifically the tolerance
    //TODO: test timeouts
  }
}
