package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random
import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.TestActor.AutoPilot
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
          task(destinationIndex = 1, result = fruit)
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
        val testCase = new TestCase[SingleTaskBundle](numberOfDestinations = 5, startingTasksIndexes = Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              // In parallel why not
              startingFruits.indices.par.foreach { i =>
                pingPong(destinations(i))
              }
              
              secondState.updatedStatuses(
                0 -> Waiting or Finished(startingFruits.map(_.length))
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
              
              expectInnerOrchestratorTermination(0)
              
              thirdState.updatedStatuses(
                0 -> Finished(startingFruits.map(_.length))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      
      //A -> N*B
      "there's a single bundle with a dependency" in {
        val testCase = new TestCase[TaskBundleDependency](numberOfDestinations = 6, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"

              secondState.updatedStatuses(
                0 -> Finished(startingFruits),
                1 -> Unstarted or Waiting
              )
            }, { thirdState =>
              // Destinations of B tasks
              startingFruits.indices.par.foreach { i =>
                // See the first test in this suite to understand why the timeout error is being ignored
                pingPong(destinations(i + 1), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination(1)
              
              thirdState.updatedStatuses(
                1 -> Finished(startingFruits.map(_.length))
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
        val testCase = new TestCase[ComplexTaskBundle](numberOfDestinations = 4, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
          
              secondState.updatedStatuses(
                0 -> Finished(startingFruits),
                1 -> Unstarted or Waiting,
                2 -> Unstarted or Waiting
              )
            }, { thirdState =>
              startingFruits.par.foreach { _ =>
                // See the first test in this suite to understand why the timeout error is being ignored
                pingPong(destinations(1), ignoreTimeoutError = true) // Destinations of B tasks
                pingPong(destinations(2), ignoreTimeoutError = true) // Destinations of C tasks
              }
    
              expectInnerOrchestratorTermination(2)
              expectInnerOrchestratorTermination(1)
              
              thirdState.updatedStatuses(
                1 -> Finished(startingFruits),
                2 -> Finished(startingFruits),
                3 -> Unstarted or Waiting
              )
            }, { fourthState =>
              (0 until startingFruits.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destinations of D tasks
              }
    
              expectInnerOrchestratorTermination(3)
              
              fourthState.updatedStatuses(
                3 -> Finished(startingFruits ++ startingFruits)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: B handled then C handled" in {
        val testCase = new TestCase[ComplexBFirstTaskBundle](numberOfDestinations = 4, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
          
              secondState.updatedStatuses(
                0 -> Finished(startingFruits),
                1 -> Unstarted or Waiting,
                2 -> Unstarted or Waiting
              )
            }, { thirdState =>
              // B tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(1))
              }
          
              expectInnerOrchestratorTermination(1)
    
              // C tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(2))
              }
              
              thirdState.updatedStatuses(
                1 -> Finished(startingFruits),
                2 -> Waiting,
                3 -> Unstarted
              )
            }, { fourthState =>
              // C tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(2), ignoreTimeoutError = true)
              }
              
              expectInnerOrchestratorTermination(2)
              
              // This proves the tasks of bundle B won't send any messages to their destination.
              // This will happen because B will recover and never trigger the creation of the inner orchestrator.
              startingFruits.par.foreach { _ =>
                destinations(1).expectNoMessage()
              }

              destinations(3).setAutoPilot(new AutoPilot {
                override def run(sender: ActorRef, msg: Any) = msg match {
                  case s: SimpleMessage =>
                    sender ! s
                    keepRunning
                }
              })

              fourthState.updatedStatuses(
                2 -> Finished(startingFruits),
                3 -> Waiting
              )
            }, { fifthState =>
              expectInnerOrchestratorTermination(3)

              fifthState.updatedStatuses(
                3 -> Finished(startingFruits ++ startingFruits)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: C handled then B handled" in {
        val testCase = new TestCase[ComplexCFirstTaskBundle](numberOfDestinations = 4, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
          
              secondState.updatedStatuses(
                0 -> Finished(startingFruits),
                1 -> Unstarted or Waiting,
                2 -> Unstarted or Waiting
              )
            }, { thirdState =>
              // C tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(2))
              }
    
              expectInnerOrchestratorTermination(2)
    
              // B tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(1))
              }
    
              thirdState.updatedStatuses(
                1 -> Waiting,
                2 -> Finished(startingFruits),
                3 -> Unstarted
              )
            }, { fourthState =>
              // B tasks
              startingFruits.par.foreach { _ =>
                pingPong(destinations(1), ignoreTimeoutError = true)
              }
    
              expectInnerOrchestratorTermination(2)

              // This proves the tasks of bundle C won't send any messages to their destination.
              // This will happen because C will recover and never send the message to the Spawner.
              startingFruits.par.foreach { _ =>
                destinations(2).expectNoMessage()
              }

              destinations(3).setAutoPilot(new AutoPilot {
                override def run(sender: ActorRef, msg: Any) = msg match {
                  case s: SimpleMessage =>
                    sender ! s
                    keepRunning
                }
              })
    
              fourthState.updatedStatuses(
                1 -> Finished(startingFruits),
                3 -> Waiting
              )
            }, { fifthState =>
              expectInnerOrchestratorTermination(3)
              
              fifthState.updatedStatuses(
                3 -> Finished(startingFruits ++ startingFruits)
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
        val testCase = new TestCase[InnerTaskAbortingTaskBundle](numberOfDestinations = 6, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              pingPong(destinations(0)) // Destination of Task "A"
              
              secondState.updatedStatuses(
                0 -> Finished(startingFruits),
                1 -> Unstarted or Waiting
              )
            }, { thirdState =>
              startingFruits.indices.par.foreach { i =>
                pingPong(destinations(i + 1))
              }
              
              expectInnerOrchestratorTermination(1)
              
              thirdState.updatedStatuses(
                1 -> Aborted(testsAbortReason)
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
        val testCase = new TestCase[OuterTaskAbortingTaskBundle](numberOfDestinations = 3, Set(0)) {
          val transformations = withStartAndFinishTransformations(
            { secondState =>
              (0 until 3).par.foreach { i =>
                // Just receive the messages without answering to ensure the outer task timeouts
                // We are ignoring the timeout. See the test "there's a single bundle" to understand why.
                pingPong(destinations(i), ignoreTimeoutError = true, pong = false)
              }
  
              import scala.concurrent.TimeoutException
              secondState.updatedStatuses(
                0 -> Aborted(new TimeoutException())
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