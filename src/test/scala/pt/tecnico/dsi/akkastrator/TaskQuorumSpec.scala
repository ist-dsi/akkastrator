package pt.tecnico.dsi.akkastrator

import akka.actor.ActorRef
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.ActorSysSpec.{ControllableOrchestrator, OrchestratorAborted}
import pt.tecnico.dsi.akkastrator.Work._
import pt.tecnico.dsi.akkastrator.TaskQuorumSpec._

object TaskQuorumSpec {
  val aResult = Seq("Farfalhi", "Kunami", "Funini", "Katuki", "Maracaté")
  
  /*
  
  class TasksWithDependenciesQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> Error B
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskQuorum("B", dependencies = Set(a), a.result.get) { s =>
      task(s, destinations(1).ref.path, s.length, dependencies = Set(a))(_)
    }
  }
  
  */
  
  class TasksWithSameDestinationQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // Error A
    /*TaskQuorum("A", dependencies = Set.empty)(
      task("0", destinations(0).ref.path, "0")(_),
      task("1", destinations(0).ref.path, "1")(_)
    )*/
  }
  
  /*
  
  class SimpleTaskQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    // A -> N*B
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskQuorum("B", dependencies = Set(a), a.result.get) { s =>
      task(s, destinations(1).ref.path, s.length)(_)
    }
  }
  class ComplexTaskQuorumOrchestrator(destinations: Array[TestProbe], probe: ActorRef) extends ControllableOrchestrator(probe) {
    //     N*B
    // A →⟨   ⟩→ 2*N*D
    //     N*C
    val a = task("A", destinations(0).ref.path, aResult)
    val b = TaskQuorum("B", dependencies = Set(a), a.result.get){ s =>
      task(s, destinations(1).ref.path, s)(_)
    }
    val c = TaskQuorum("C", dependencies = Set(a), a.result.get){ s =>
      task(s, destinations(2).ref.path, s)(_)
    }
    val d = TaskQuorum("D", dependencies = Set(b, c), b.result.get ++ c.result.get) { s =>
      task(s, destinations(3).ref.path, s)(_)
    }
  }
  
  */
}
class TaskQuorumSpec extends ActorSysSpec {
  /*
  
  "An orchestrator with task quorum" should {
    "must fail" when {
      /*
      "the tasksCreator generates tasks with dependencies" in {
        val testCase = new TestCase[TasksWithDependenciesQuorumOrchestrator](2, Set('A)) {
          val transformations: Seq[(State) => State] = Seq(
            { secondState =>
              pingPongDestinationOf('A)
              
              secondState.updatedExactStatuses(
                'A -> Finished(aResult)
              ).updatedStatuses(
                'B -> Set(Unstarted, Waiting)
              )
            }, { thirdState =>
              terminationProbe.expectMsg(OrchestratorAborted)
              
              thirdState.updatedExactStatuses(
                'B -> Aborted(InitializationError("TasksCreator must generate tasks without dependencies."))
              )
            }
          )
        }
        testCase.testRecovery()
      }
      */
      "the tasksCreator generates tasks with the same destination" in {
        val testCase = new TestCase[TasksWithSameDestinationQuorumOrchestrator](2, Set("A")) {
          val transformations: Seq[(State) => State] = Seq(
            { secondState =>
              terminationProbe.expectMsg(OrchestratorAborted)
              
              secondState.updatedExactStatuses(
                "A" -> Aborted(InitializationError("TasksCreator must generate tasks with the same destination."))
              )
            }
          )
        }
        testCase.testExpectedStatusWithRecovery()
      }
    }
    /*
    "behave according to the documentation" when {
      "there is only a single task bundler: A -> N*B" in {
        val testCase = new TestCase[SimpleTaskQuorumOrchestrator](2, Set('A)) {
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
      
      """there are two bundles:
        |     N*B
        | A →⟨   ⟩→ 2*N*D
        |     N*C
      """.stripMargin in {
        val testCase = new TestCase[ComplexTaskQuorumOrchestrator](4, Set('A)) {
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
    */
  }
  
  */
}