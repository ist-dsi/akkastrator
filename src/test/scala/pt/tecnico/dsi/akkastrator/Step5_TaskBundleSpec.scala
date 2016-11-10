package pt.tecnico.dsi.akkastrator

import akka.actor.{ActorPath, ActorRef}
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, OrchestratorAborted, testsAbortReason}
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.Step5_TaskBundleSpec._
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import shapeless.{::, HNil}

object Step5_TaskBundleSpec {
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
  
  // A -> N*B
  class SimpleTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = simpleMessagefulltask("A", destinations(0), aResult)
    val b = FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskBundle(_)(o =>
        fruits.zipWithIndex.map { case (fruit, i) =>
          //Declaring the inner task directly in the body of the task bundle.
          //NOTE: unfortunately its necessary to pass the orchestrator o explicitly.
          FullTask(fruit).createTaskWith { case HNil =>
            new Task[Int](_) {
              val destination: ActorPath = destinations(i + 1).ref.path
              def createMessage(id: Long): Any = SimpleMessage(fruit, id)
              def behavior: Receive = {
                case m @ SimpleMessage(_, id) if matchId(id) =>
                  finish(m, id, fruit.length)
              }
            }
          }(o)
        }
      )
    }
  }
  
  //     N*B
  // A →⟨   ⟩→ 2N*D
  //     N*C
  class ComplexTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = simpleMessagefulltask("A", destinations(0), aResult)
    val b = FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskBundle(_)(o =>
        fruits.map { fruit =>
          //Using a method that creates a full task.
          //NOTE: unfortunately its necessary to pass the orchestrator o explicitly.
          // We are using the default value for Taskcommaped.
          simpleMessagefulltask(s"$fruit-B", destinations(1), fruit)(o)
        }
      )
    }
    val c = FullTask("C", a :: HNil) createTaskWith { case fruits :: HNil =>
      new TaskBundle(_)(o =>
        fruits.map { fruit =>
          simpleMessagefulltask(s"$fruit-C", destinations(2), fruit)(o)
        }
      )
    }
    val d = FullTask("D", b :: c :: HNil) createTaskWith { case fruitsB :: fruitsC :: HNil =>
      new TaskBundle(_)(o =>
        (fruitsB ++ fruitsC) map { fruit =>
          simpleMessagefulltask(s"$fruit-D", destinations(3), fruit)(o)
        }
      )
    }
  }
  class ComplexBFirstTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ComplexTaskBundleOrchestrator(destinations, probe)
  class ComplexCFirstTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ComplexTaskBundleOrchestrator(destinations, probe)
  
  // A -> N*B (one of B aborts)
  class AbortingTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = simpleMessagefulltask("A", destinations(0), aResult)
    FullTask("B", a :: HNil) createTaskWith { case fruits :: HNil =>
        new TaskBundle(_)(o =>
          Seq(
            simpleMessagefulltask("Farfalhi", destinations(1), "Farfalhi")(o),
            simpleMessagefulltask("Kunami", destinations(2), "Kunami")(o),
            simpleMessagefulltask("Funini", destinations(3), "Funini", abortOnReceive = true)(o),
            simpleMessagefulltask("Katuki", destinations(4), "Katuki")(o),
            simpleMessagefulltask("Maracaté", destinations(5), "Maracaté")(o)
          )
        )
    }
  }
}
class Step5_TaskBundleSpec extends ActorSysSpec {
  "An orchestrator with task bundles" should {
    "behave according to the documentation" when {
      //A -> N*B
      "there's a single bundle" in {
        val testCase = new TestCase[SimpleTaskBundleOrchestrator](6, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              handleResend("A")
    
              //In parallel why not
              aResult.indices.par.foreach { i =>
                pingPong(destinations(i + 1)) // Destinations of B tasks
              }
              
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedExactStatuses(
                "B" -> Finished(aResult.map(_.length))
              )
            }, { fourthState =>
              // Note that even with the orchestrator crashing the inner orchestrator won't run again.
              // This is consistent with the orchestrator recovering since the task B (the task bundle) will
              // recover the MessageReceived, thus it will never send the SpawnAndStart message.
              // Which in turn means the inner orchestrator will never be created.
              aResult.indices.par.foreach { i =>
                destinations(i + 1).expectNoMsg()
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
        val testCase = new TestCase[ComplexTaskBundleOrchestrator](4, Set("A")) {
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
              handleResend("A")
              
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1)) // Destinations of B tasks
                pingPong(destinations(2)) // Destinations of C tasks
              }
    
              expectInnerOrchestratorTermination("C")
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedExactStatuses(
                "B" -> Finished(aResult),
                "C" -> Finished(aResult)
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fourthState =>
              //In parallel why not
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destinations of D tasks
              }
    
              expectInnerOrchestratorTermination("D")
              
              fourthState.updatedExactStatuses(
                "D" -> Finished(aResult ++ aResult)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: B handled then C handled" in {
        val testCase = new TestCase[ComplexBFirstTaskBundleOrchestrator](4, Set("A")) {
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
              handleResend("A")
          
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1)) // Destination of B tasks
              }
          
              expectInnerOrchestratorTermination("B")
              logger.info("Inner of B finished")
          
              thirdState.updatedExactStatuses(
                "B" -> Finished(aResult)
              ).updatedStatuses(
                "C" -> Set(Waiting, Finished(aResult)),
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fourthState =>
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(2)) // Destination of C tasks
              }
              aResult.par.foreach { _ =>
                pingPong(destinations(2)) // Destination of C tasks
              }
              
              expectInnerOrchestratorTermination("C")
              logger.info("Inner of C finished")
    
              // Since the task bundle B recovered its MessageReceive its tasks won't send any
              // message to their destination. This proves that is what happens.
              aResult.par.foreach { _ =>
                destinations(1).expectNoMsg()
              }
    
              fourthState.updatedExactStatuses(
                "C" -> Finished(aResult)
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fifthState =>
              //In parallel why not
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destination of D tasks
              }
              //In parallel why not
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destination of D tasks
              }
          
              expectInnerOrchestratorTermination("D")
              logger.info("Inner of D finished")
              
              fifthState.updatedExactStatuses(
                "D" -> Finished(aResult ++ aResult)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
      "there are three bundles: C handled then B handled" in {
        val testCase = new TestCase[ComplexCFirstTaskBundleOrchestrator](4, Set("A")) {
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
              handleResend("A")
              
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(2)) // Destination of C tasks
              }
          
              expectInnerOrchestratorTermination("C")
              logger.info("Inner of C finished")
          
              thirdState.updatedExactStatuses(
                "C" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Waiting, Finished(aResult)),
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fourthState =>
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1)) // Destination of B tasks
              }
              aResult.par.foreach { _ =>
                pingPong(destinations(1)) // Destination of B tasks
              }
    
              expectInnerOrchestratorTermination("B")
              logger.info("Inner of B finished")
          
              // Since the task bundle C recovered its MessageReceive its tasks won't send any
              // message to their destination. This proves that is what happens.
              aResult.par.foreach { _ =>
                destinations(2).expectNoMsg()
              }
          
              fourthState.updatedExactStatuses(
                "C" -> Finished(aResult)
              ).updatedStatuses(
                "D" -> Set(Unstarted, Waiting)
              )
            }, { fifthState =>
              //In parallel why not
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destination of D tasks
              }
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3)) // Destination of D tasks
              }
          
              expectInnerOrchestratorTermination("D")
              logger.info("Inner of D finished")
          
              fifthState.updatedExactStatuses(
                "D" -> Finished(aResult ++ aResult)
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    "should abort" when {
      "when an inner task aborts" in {
        val testCase = new TestCase[AbortingTaskBundleOrchestrator](6, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPong("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              handleResend("A")
              
              //In parallel why not
              aResult.indices.par.foreach { i =>
                pingPong(destinations(i + 1))
              }
              
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedExactStatuses(
                "B" -> Aborted(testsAbortReason)
              )
            }, { fourthState =>
              //Because the inner orchestrator of the bundle wont be started the destinations wont receive resends
              /*aResult.indices.par.foreach { i =>
                pingPong(destinations(i + 1))
              }*/
              
              terminationProbe.expectMsg(OrchestratorAborted)
              
              fourthState
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    //TODO: test timeouts
  }
}