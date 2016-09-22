package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, OrchestratorAborted}
import pt.tecnico.dsi.akkastrator.Task._
import pt.tecnico.dsi.akkastrator.TaskBundleSpec._

object TaskBundleSpec {
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
 
  class SimpleTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> N*B
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskBundle("B", dependencies = Set(a), a.result.get) { s =>
      task(s, destinations(1).ref.path, s.length)(_)
    }
  }
  class ComplexTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //     N*B
    // A →⟨   ⟩→ 2*N*D
    //     N*C
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskBundle("B", dependencies = Set(a), a.result.get){ s =>
      task(s, destinations(1).ref.path, s)(_)
    }
    val c = TaskBundle("C", dependencies = Set(a), a.result.get){ s =>
      task(s, destinations(2).ref.path, s)(_)
    }
    val d = TaskBundle("D", dependencies = Set(b, c), b.result.get ++ c.result.get) { s =>
      task(s, destinations(3).ref.path, s)(_)
    }
  }
}
class TaskBundleSpec extends ActorSysSpec {
  "An orchestrator with task bundles" should {
    "behave according to the documentation" when {
      "there is only a single task bundler: A -> N*B" in {
        val testCase = new TestCase[SimpleTaskBundleOrchestrator](2, Set('A)) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPongDestinationOf('A)
              
              secondState.updatedExactStatuses(
                'A -> Finished(aResult)
              )
            }, { thirdState =>
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1))
              }
              
              //Resend of A
              pingPongDestinationOf('A)
    
              //We cannot directly control the inner orchestrator here,
              //so we need to give some time to finish
              //Thread.sleep(50)
      
              thirdState.updatedStatuses(
                'B -> Set(Waiting, Finished(aResult.map(_.length)))
              )
            }
          )
        }
        testCase.testRecovery()
      }
      
      """there are three bundles:
        |     N*B
        | A →⟨   ⟩→ 2*N*D
        |     N*C
      """.stripMargin in {
        val testCase = new TestCase[ComplexTaskBundleOrchestrator](4, Set('A)) {
          val transformations: Seq[State => State] = Seq(
            { secondState =>
              pingPongDestinationOf('A)
          
              secondState.updatedExactStatuses(
                'A -> Finished(aResult)
              ).updatedStatuses(
                'B -> Set(Unstarted, Waiting),
                'C -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              //In parallel why not
              aResult.par.foreach { _ =>
                pingPong(destinations(1))
                pingPong(destinations(2))
              }
    
              //Re-send of A
              pingPongDestinationOf('A)
              
              //We cannot directly control the inner orchestrator here,
              //so we need to give some time to finish
              Thread.sleep(100)
          
              thirdState.updatedStatuses(
                'B -> Set(Waiting, Finished(aResult)),
                'C -> Set(Waiting, Finished(aResult)),
                'D -> Set(Unstarted, Waiting)
              )
            }, { fourthState =>
              //In parallel why not
              (0 until aResult.length * 2).par.foreach { _ =>
                pingPong(destinations(3))
              }
    
              //We cannot directly control the inner orchestrator here,
              //so we need to give some time to finish
              Thread.sleep(100)
              
              fourthState.updatedExactStatuses(
                'B -> Finished(aResult),
                'C -> Finished(aResult)
              ).updatedStatuses(
                'D -> Set(Waiting, Finished(aResult ++ aResult))
              )
            }
          )
        }
        testCase.testRecovery()
      }
    }
  }
}
