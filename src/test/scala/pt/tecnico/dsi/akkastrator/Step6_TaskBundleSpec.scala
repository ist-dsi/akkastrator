package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random

import akka.actor.{Actor, Props}
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec._
import pt.tecnico.dsi.akkastrator.DSL.{FullTask, TaskBundle, TaskSpawnOrchestrator}
import pt.tecnico.dsi.akkastrator.Step6_TaskBundleSpec._
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.{::, HNil}

object Step6_TaskBundleSpec {
  class DummyActor extends Actor {
    def receive: Receive = Actor.emptyBehavior
  }
  
  class InvalidTaskSpawnOrchestrator(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    FullTask("A") createTask { _ =>
      // We are stating that TaskSpawnOrchestrator will create a Bundle[Int] orchestrator
      // which satisfies the type constraints of the TaskSpawnOrchestrator, but then we provide
      // it with a Props class that creates a different actor which does not satisfies the type constraints.
      // This test serves to catch this error.
      TaskSpawnOrchestrator[Seq[Int], Bundle[Int]](Props[DummyActor])
    }
  }
  
  val startingFruits = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
  
  // N*B
  class SingleTaskBundle(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    // In practice an orchestrator with a single TaskBundle like this one is useless.
    // We created it because it serves us as a sort of incremental test ramping up to a "complex" orchestrator.
    // If this test fails then there is a problem that is inherent to task bundles and not to some sort of
    // interplay between some other akkastrator abstraction.
  
    FullTask("A") createTask { _ =>
      TaskBundle(
        startingFruits.zipWithIndex.map { case (fruit, i) =>
          task(destinationIndex = i, result = fruit.length)
        }
      )
    }
  }
  
  // A -> N*B
  class TaskBundleDependency(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0, startingFruits)
  
    val b = FullTask("B", a) createTaskWith { case fruits :: HNil =>
      TaskBundle(
        fruits.zipWithIndex.map { case (fruit, i) =>
          task(destinationIndex = i + 1, result = fruit.length)
        }
      )
    }
  }
  
  //     N*B
  // A →⟨   ⟩→ 2N*D
  //     N*C
  class ComplexTaskBundle(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0, startingFruits)
    val b = FullTask("B", a) createTaskWith { case fruits :: HNil =>
      TaskBundle(
        fruits map { fruit =>
          task(destinationIndex =1, result = fruit)
        }
      )
    }
    val c = FullTask("C", a :: HNil, Duration.Inf) createTaskWith { case fruits :: HNil =>
      TaskBundle(
        fruits.map { fruit =>
          task(destinationIndex = 2, result = fruit)
        }
      )
    }
    val d = FullTask("D", (b, c)) createTask { case (fruitsB, fruitsC) =>
      TaskBundle(
        (fruitsB ++ fruitsC) map { fruit =>
          task(destinationIndex = 3, result = fruit)
        }
      )
    }
  }
  class ComplexBFirstTaskBundle(destinations: Array[TestProbe]) extends ComplexTaskBundle(destinations)
  class ComplexCFirstTaskBundle(destinations: Array[TestProbe]) extends ComplexTaskBundle(destinations)
  
  // A -> N*B (one of B aborts)
  class InnerTaskAbortingTaskBundle(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    val a = simpleMessageFulltask("A", 0, startingFruits)
  
    val abortingTask = Random.nextInt(5)
    
    FullTask("B", a) createTaskWith { case fruits :: HNil =>
      TaskBundle(
        fruits.zipWithIndex.map { case (fruit, i) =>
          task(destinationIndex = i + 1, result = fruit, abortOnReceive = i == abortingTask)
        }
      )
    }
  }
  // N*A (A timeouts)
  class OuterTaskAbortingTaskBundle(destinations: Array[TestProbe]) extends ControllableOrchestrator(destinations) {
    FullTask("A", timeout = 1.millis) createTaskWith { _ =>
      TaskBundle (
        task(destinationIndex = 0),
        task(destinationIndex = 1),
        task(destinationIndex = 2)
      )
    }
  }
}
class Step6_TaskBundleSpec extends ActorSysSpec {
  /*
  "A TaskSpwanOrchestrator" should {
    "throw an exception" when {
      "the Props pass does not match the type arguments specified" in {
        val testCase = new TestCase[InvalidTaskSpawnOrchestrator](numberOfDestinations = 0, Set("A")) {
          val transformations = Seq(startTransformation)
        }
        import testCase._
        testCase.differentTestPerState(
          { testStatus(_) }, // 1st state: startingTasks -> Unstarted.
          // StartOrchestrator is sent
          { secondState =>
            ()
          }
        )
      }
    }
  }
  */
  "An orchestrator with task bundles" should {
    "execute the tasks of the inner orchestrator" when {
      //N*A
      "there's a single bundle" in {
        val testCase = new TestCase[SingleTaskBundle](numberOfDestinations = 5, startingTasks = Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              // In parallel why not
              startingFruits.indices.par.foreach { i =>
                pingPong(destinations(i))
              }
              
              secondState.updatedStatuses(
                "A" -> Waiting or Finished(startingFruits.map(_.length))
              )
            }, { thirdState =>
              // By this time some of the inner tasks of A might have already finished (we don't know which, if any).
              // The ones that have finished will not send a message to their destination,
              // however the ones that are still waiting will.
              //  If we don't pingPong for the waiting ones the entire test will fail since
              //   the inner orchestrator will never terminate.
              //  If we try to pingPong for the finished ones the expectMsg will timeout and throw an exception
              //   causing the test to erroneously fail.
              // To get out of this pickle we pingPong every destination but ignore any timeout error.
              startingFruits.indices.par.foreach { i =>
                pingPong(destinations(i), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination("A")
              
              thirdState.updatedStatuses(
                "A" -> Finished(startingFruits.map(_.length))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      
      //A -> N*B
      "there's a single bundle with a dependency" in {
        val testCase = new TestCase[TaskBundleDependency](numberOfDestinations = 6, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
              
              secondState.updatedStatuses(
                "A" -> Finished(startingFruits),
                "B" -> Unstarted or Waiting
              )
            }, { thirdState =>
              handleResend("A")
              
              // Destinations of B tasks
              startingFruits.indices.par.foreach { i =>
                // See the first test in this suite to understand why the timeout error is being ignored
                pingPong(destinations(i + 1), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedStatuses(
                "B" -> Finished(startingFruits.map(_.length))
              )
            }, { fourthState =>
              // Note that even with the orchestrator crashing the inner orchestrator won't run again.
              // This is consistent with the orchestrator recovering since the task B (the task bundle) will
              // recover the TaskFinished, thus it will never send the SpawnAndStart message.
              // Which in turn means the inner orchestrator will never be created.
              startingFruits.indices.par.foreach { i =>
                destinations(i + 1).expectNoMessage()
              }
              fourthState
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      
      //     N*B
      // A →⟨   ⟩→ 2*N*D
      //     N*C
      "there are three bundles: B and C handled at same time" in {
        val testCase = new TestCase[ComplexTaskBundle](numberOfDestinations = 4, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
          
              secondState.updatedStatuses(
                "A" -> Finished(startingFruits),
                "B" -> Unstarted or Waiting,
                "C" -> Unstarted or Waiting
              )
            }, { thirdState =>
              handleResend("A")
              
              startingFruits.par.foreach { _ =>
                // See the first test in this suite to understand why the timeout error is being ignored
                pingPong(destinations(1), ignoreTimeoutError = true) // Destinations of B tasks
                pingPong(destinations(2), ignoreTimeoutError = true) // Destinations of C tasks
              }
    
              expectInnerOrchestratorTermination("C")
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedStatuses(
                "B" -> Finished(startingFruits),
                "C" -> Finished(startingFruits),
                "D" -> Unstarted or Waiting
              )
            }, { fourthState =>
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destinations of D tasks
              }
    
              expectInnerOrchestratorTermination("D")
              
              fourthState.updatedStatuses(
                "D" -> Finished(startingFruits ++ startingFruits)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: B handled then C handled" in {
        val testCase = new TestCase[ComplexBFirstTaskBundle](numberOfDestinations = 4, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
          
              secondState.updatedStatuses(
                "A" -> Finished(startingFruits),
                "B" -> Unstarted or Waiting,
                "C" -> Unstarted or Waiting
              )
            }, { thirdState =>
              handleResend("A")
    
              // B tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(1))
              }
          
              expectInnerOrchestratorTermination("B")
    
              // C tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(2))
              }
              
              thirdState.updatedStatuses(
                "B" -> Finished(startingFruits),
                "C" -> Waiting,
                "D" -> Unstarted
              )
            }, { fourthState =>
              // C tasks
              startingFruits.par.foreach { _ =>
                handleResend(destinations(2), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination("C")
              
              // Since the task bundle B recovered its MessageReceive its tasks won't send any
              // message to their destination. This proves that is what happens.
              startingFruits.par.foreach { _ =>
                destinations(1).expectNoMessage()
              }
    
              // D tasks. The previous expectNoMessage gave enough time for some of D tasks to start,
              // so we pingPong them to use up their messages. We say use up because the orchestrator
              // will crash right away and cause the sender() reference in the pingPong to become invalid.
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3))
              }
              
              fourthState.updatedStatuses(
                "C" -> Finished(startingFruits),
                "D" -> Waiting
              )
            }, { fifthState =>
              // D tasks
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3), ignoreTimeoutError = true)
                handleResend(destinations(3), ignoreTimeoutError = true)
              }
          
              expectInnerOrchestratorTermination("D")
              
              fifthState.updatedStatuses(
                "D" -> Finished(startingFruits ++ startingFruits)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: C handled then B handled" in {
        val testCase = new TestCase[ComplexCFirstTaskBundle](numberOfDestinations = 4, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
          
              secondState.updatedStatuses(
                "A" -> Finished(startingFruits),
                "B" -> Unstarted or Waiting,
                "C" -> Unstarted or Waiting
              )
            }, { thirdState =>
              handleResend("A")
    
              // C tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(2))
              }
    
              expectInnerOrchestratorTermination("C")
    
              // B tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(1))
              }
    
              thirdState.updatedStatuses(
                "B" -> Waiting,
                "C" -> Finished(startingFruits),
                "D" -> Unstarted
              )
            }, { fourthState =>
              // B tasks
              startingFruits.par.foreach { _ =>
                handleResend(destinations(1), ignoreTimeoutError = true)
              }
    
              expectInnerOrchestratorTermination("B")
    
              // Since the task bundle C recovered its MessageReceive its tasks won't send any
              // message to their destination. This proves that is what happens.
              startingFruits.par.foreach { _ =>
                destinations(2).expectNoMessage()
              }
    
              // D tasks. The previous expectNoMessage gave enough time for some of D tasks to start,
              // so we pingPong them to use up their messages. We say use up because the orchestrator
              // will crash right away and cause the sender() reference in the pingPong to become invalid.
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3))
              }
    
              fourthState.updatedStatuses(
                "B" -> Finished(startingFruits),
                "D" -> Unstarted or Waiting
              )
            }, { fifthState =>
              // D tasks
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3))
                handleResend(destinations(3), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination("D")
              
              fifthState.updatedStatuses(
                "D" -> Finished(startingFruits ++ startingFruits)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    "should abort" when {
      // A -> N*B one of B aborts
      "when an inner task aborts" in {
        val testCase = new TestCase[InnerTaskAbortingTaskBundle](numberOfDestinations = 6, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong("A")
              
              secondState.updatedStatuses(
                "A" -> Finished(startingFruits),
                "B" -> Unstarted or Waiting
              )
            }, { thirdState =>
              handleResend("A")
              
              startingFruits.indices.par.foreach { i =>
                pingPong(destinations(i + 1))
              }
              
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedStatuses(
                "B" -> Aborted(testsAbortReason)
              )
            }, { fourthState =>
              parentProbe expectMsg OrchestratorAborted
              
              fourthState
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      // N*A the bundle timeouts
      "the bundle timeouts" in {
        val testCase = new TestCase[OuterTaskAbortingTaskBundle](numberOfDestinations = 3, Set("A")) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              (0 until 3).par.foreach { i =>
                // Just receive the messages without answering to ensure the outer task timeouts
                // We are ignoring the expectMsgType timeout. See the test "there's a single bundle" to understand why.
                try {
                  destinations(i).expectMsgType[SimpleMessage]
                } catch {
                  case e: AssertionError if e.getMessage.contains("timeout") =>
                  // Purposefully ignored
                }
              }
  
              import scala.concurrent.TimeoutException
              secondState.updatedStatuses(
                "A" -> Aborted(new TimeoutException())
              )
            }, { thirdState =>
              // While recovering the bundle A will handle the MessageReceive. Or in other words its
              // spawner won't create the inner orchestrator and therefor the inner tasks will never send their messages.
              (0 until 3).indices.par.foreach { i =>
                destinations(i).expectNoMessage()
              }
          
              parentProbe expectMsg OrchestratorAborted
          
              thirdState
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
  }
}