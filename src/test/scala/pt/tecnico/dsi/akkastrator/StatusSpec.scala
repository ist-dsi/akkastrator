package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

import scala.concurrent.duration.DurationInt

class StatusSpec extends IntegrationSpec {
  test("Case 1: a command status should be Unstarted -> Waiting -> Finished") {
    val destinationActor0 = TestProbe()

    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoTask("Command0", destinationActor0.ref.path)
    }))

    withOrchestratorTermination(orchestrator){ probe =>
      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Unstarted | Waiting, _)) => true
      }

      val a0m = destinationActor0.expectMsgClass(1.seconds, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting, _)) => true
      }

      destinationActor0.reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished | Waiting, _)) => true
      }
    }
  }

  test("Case 2: status of two sequential commands should be independent") {
    val (destinations, orchestrator) = NChainedTasksOrchestrator(2)

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Unstarted | Waiting, _),
                 TaskStatus(1, _, Unstarted, _)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      destinations(1).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting, _),
                 TaskStatus(1, _, Unstarted, _)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))
      //We can't execute the following line because the previous reply will cause the
      //2nd command to start and thus destination(1) will receive a SimpleMessage
      //destinations(1).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting | Finished, _),
                 TaskStatus(1, _, Unstarted | Waiting, _)) => true
      }

      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])
      destinations(0).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished, _),
                 TaskStatus(1, _, Waiting, _)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))
      //We can't execute the following line because the previous reply will cause the
      //last command to finish and thus the orchestrator to finish which will make it
      //impossible for us to be asking its status
      //destinations(0).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished, _),
                 TaskStatus(1, _, Finished, _)) => true
      }
    }
  }

  test("Case 3: status of two parallel commands should be independent - Command0 replies first") {
    val destinations = Array.fill(2)(TestProbe())
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoTask("Command0", destinations(0).ref.path)
      echoTask("Command1", destinations(1).ref.path)
    }))

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Unstarted | Waiting, _),
                 TaskStatus(1, _, Unstarted | Waiting, _)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting, _),
                 TaskStatus(1, _, Waiting, _)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished, _),
                 TaskStatus(1, _, Waiting, _)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished, _),
                 TaskStatus(1, _, Finished, _)) => true
      }
    }
  }

  test("Case 4: status of two parallel commands should be independent - Command1 replies first") {
    val destinations = Array.fill(2)(TestProbe())
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoTask("Command0", destinations(0).ref.path)
      echoTask("Command1", destinations(1).ref.path)
    }))

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Unstarted | Waiting, _),
                 TaskStatus(1, _, Unstarted | Waiting, _)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting, _),
                 TaskStatus(1, _, Waiting, _)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Waiting, _),
                 TaskStatus(1, _, Finished, _)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(TaskStatus(0, _, Finished, _),
                 TaskStatus(1, _, Finished, _)) => true
      }
    }
  }
}
