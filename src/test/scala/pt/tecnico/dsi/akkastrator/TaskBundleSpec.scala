package pt.tecnico.dsi.akkastrator

import akka.actor.Actor.Receive
import akka.actor.{ActorPath, ActorRef}
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.ControllableOrchestrator
import pt.tecnico.dsi.akkastrator.TaskBundleSpec._
import pt.tecnico.dsi.akkastrator.Task._

object TaskBundleSpec {
  object SimpleTaskBundleOrchestrator {
    val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
  }
  class SimpleTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> N*B
    val a = task("A", destinations(0).ref.path, SimpleTaskBundleOrchestrator.aResult)
    val b = taskBundle(a.result.get, destinations(1).ref.path, "B", Set(a))
  }
  
  /*
  object ComplexTaskBundleOrchestrator {
    val aResult = Seq("Apples", "Oranges")
  }
  class ComplexTaskBundleOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //     N*B
    // A →⟨   ⟩→ 2*N*D
    //     N*C
    val a = task("A", destinations(0).ref.path, ComplexTaskBundleOrchestrator.aResult)
    val b = taskBundle(a.result.get, destinations(1).ref.path, "B", Set(a))
    val c = taskBundle(a.result.get, destinations(2).ref.path, "C", Set(a))
    val d = taskBundle(b.result.get ++ c.result.get, destinations(3).ref.path, "D", Set(b, c))
  }
  */
}
class TaskBundleSpec extends ActorSysSpec {
  "An orchestrator with task bundles" should {
    "behave according to the documentation" when {
      "there is only a single task bundler: A -> N*B" in {
        val testCase = new TestCase[SimpleTaskBundleOrchestrator](2, Set('A)) {
          import SimpleTaskBundleOrchestrator._
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationOf('A)
              
              secondState.updatedExactStatuses(
                'A → Finished(aResult)
              )
            }, { thirdState ⇒
              aResult.indices.foreach { _ ⇒
                val m = destinations(1).expectMsgClass(classOf[SimpleMessage])
                destinations(1).reply(m)
              }
              //Resend of A
              pingPongDestinationOf('A)
      
              //Give some time for the inner orchestrator tasks to finish
              Thread.sleep(100) //FIXME: remove the sleep
              
              thirdState.updatedStatuses(
                'B → Set(Waiting, Finished(aResult))
              )
            }
          )
        }
        
        testCase.testRecovery()
      }
      
      /*
      """there are two bundles:
        |     N*B
        | A →⟨   ⟩→ 2*N*D
        |     N*C
      """.stripMargin in {
        val testCase = new TestCase[ComplexTaskBundleOrchestrator](4, Set('A)) {
          import ComplexTaskBundleOrchestrator._
          val transformations: Seq[State ⇒ State] = Seq(
            { secondState ⇒
              pingPongDestinationOf('A)
          
              secondState.updatedExactStatuses(
                'A → Finished(aResult)
              ).updatedStatuses(
                'B → Set(Unstarted, Waiting),
                'C → Set(Unstarted, Waiting)
              )
            }, { thirdState ⇒
              //Re-send of A
              pingPongDestinationOf('A)
              
              aResult.indices.foreach { _ ⇒
                val mB = destinations(1).expectMsgClass(classOf[SimpleMessage])
                destinations(1).reply(mB)
  
                val mC = destinations(2).expectMsgClass(classOf[SimpleMessage])
                destinations(2).reply(mC)
              }
              
              //Give some time for the inner orchestrator tasks to finish
              Thread.sleep(100) //FIXME: remove the sleep
          
              thirdState.updatedStatuses(
                'B → Set(Waiting, Finished(aResult)),
                'C → Set(Waiting, Finished(aResult)),
                'D → Set(Unstarted, Waiting)
              )
            }, { fourthState ⇒
              for(_ ← 0 until aResult.length * 2) {
                val mD = destinations(3).expectMsgClass(classOf[SimpleMessage])
                destinations(3).reply(mD)
              }
    
              //Give some time for the inner orchestrator tasks to finish
              Thread.sleep(300) //FIXME: remove the sleep
              
              fourthState.updatedExactStatuses(
                'B → Finished(aResult),
                'C → Finished(aResult),
                'D → Finished(aResult ++ aResult)
              )
            }
          )
        }
        
        testCase.testRecovery()
      }
      */
    }
  }
}
