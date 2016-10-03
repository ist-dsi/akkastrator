package pt.tecnico.dsi.akkastrator

import akka.actor.{ActorPath, ActorRef}
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, OrchestratorAborted, testsAbortReason}
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.TaskBundleSpec._
import scala.concurrent.duration.DurationInt

import scala.concurrent.duration.Duration

object TaskBundleSpec {
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
  
  // A -> N*B
  class SimpleTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = task("A", destinations(0).ref.path, aResult)
    val b = new TaskBundle("B", dependencies = Set(a))(o =>
      a.result.get.zipWithIndex.map { case (s, i) =>
        //This implementation shows that we can implement the inner task here.
        //In this implementation only behavior and description depends on "s", but destination and createMessage could depend as well.
        new Task[Int](s)(o) { //The second argument list with the underscore is vital for this to work
          val destination: ActorPath = destinations(i + 1).ref.path
          def createMessage(id: Long): Any = SimpleMessage(description, id)
    
          def behavior: Receive =  {
            case m @ SimpleMessage(_, id) if matchId(id) =>
              finish(m, id, s.length)
          }
        }
      }
    )
  }
  //     N*B
  // A →⟨   ⟩→ 2*N*D
  //     N*C
  class ComplexTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskBundle("B", dependencies = Set(a))(a.result.get) { s =>
      task(s"$s-B", destinations(1).ref.path, s)(_)
    }
    //Alternative way. Instead of using _ for the implicit orchestrator argument, we make it explicit via currying.
    val c = TaskBundle("C", Set(a))(a.result.get) { s => o =>
      task(s"$s-C", destinations(2).ref.path, s)(o)
    }
    //Using the constructor directly. Watch out for the second argument list, which is enclosed in parenthesis and not in curly braces.
    val d = new TaskBundle("D", dependencies = Set(b, c))(o =>
      (b.result.get ++ c.result.get).map { s =>
        task(s, destinations(3).ref.path, s)(o)
      }
    )
  }
  class ComplexBFirstTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ComplexTaskBundleOrchestrator(destinations, probe)
  class ComplexCFirstTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ComplexTaskBundleOrchestrator(destinations, probe)
  
  // A -> N*B (of of B aborts)
  class AbortingTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    val a = task("A", destinations(0).ref.path, aResult)
    val b = new TaskBundle("B", Set(a): Set[Task[_]], Duration.Inf)(o =>
      Seq(
        task("Farfalhi", destinations(1).ref.path, "Farfalhi")(o),
        task("Kunami", destinations(1).ref.path, "Kunami")(o),
        task("Funini", destinations(1).ref.path, "Funini", abortOnReceive = true)(o),
        task("Katuki", destinations(1).ref.path, "Katuki")(o),
        task("Maracaté", destinations(1).ref.path, "Maracaté")(o)
      )
    )
  }
}
class TaskBundleSpec extends ActorSysSpec {
  //Because we are reusing the ComplexTaskBundleOrchestrator we also need to clean the storage locations after each test
  "An orchestrator with task bundles" should {
    "behave according to the documentation" when {
      //A -> N*B
      "there's a single bundle" in {
        val testCase = new TestCase[SimpleTaskBundleOrchestrator](6, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPongTestProbeOf("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //Resend of A
              pingPongTestProbeOf("A")
    
              //In parallel why not
              (1 to 5).par.foreach { i =>
                pingPong(destinations(i)) // Destinations of B tasks
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
              (1 to 5).par.foreach { i =>
                destinations(i).expectNoMsg()
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
              pingPongTestProbeOf("A")
          
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting),
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //Re-send of A
              pingPongTestProbeOf("A")
              
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
              pingPongTestProbeOf("A")
          
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting),
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //Re-send of A
              pingPongTestProbeOf("A")
          
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
              pingPongTestProbeOf("A")
          
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting),
                "C" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //Resend of A
              pingPongTestProbeOf("A")
              
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
        val testCase = new TestCase[AbortingTaskBundleOrchestrator](2, Set("A")) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPongTestProbeOf("A")
              
              secondState.updatedExactStatuses(
                "A" -> Finished(aResult)
              ).updatedStatuses(
                "B" -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //Resend of A
              pingPongTestProbeOf("A")
              
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1))
              }
              
              expectInnerOrchestratorTermination("B")
              
              thirdState.updatedExactStatuses(
                "B" -> Aborted(testsAbortReason)
              )
            }, { fourthState =>
              aResult.par.foreach { _ =>
                destinations(1).expectNoMsg()
              }
              
              terminationProbe.expectMsg(OrchestratorAborted)
    
              fourthState
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
  }
}
