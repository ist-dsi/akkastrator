package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import akka.testkit.TestProbe
import pt.tecnico.dsi.akkastrator.Task.{Finished, Unstarted, Waiting}

import scala.concurrent.duration.DurationInt

class BusinessRetryAndStatusSpec extends IntegrationSpec {
  test("Case 1: a command status should be Unstarted -> Waiting -> Finished") {
    val destinationActor0 = TestProbe()

    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoCommand("Command0", destinationActor0.ref.path, SimpleMessage)
    }))

    withOrchestratorTermination(orchestrator){ probe =>
      testStatus(orchestrator, probe) {
        case Seq(Task(_, Unstarted | Waiting, 0)) => true
      }

      val a0m = destinationActor0.expectMsgClass(1.seconds, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting | Waiting, 0)) => true
      }

      destinationActor0.reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished | Waiting, 0)) => true
      }
    }
  }

  test("Case 2: status of two sequential commands should be independent") {
    val (destinations, orchestrator) = NChainedCommandsOrchestrator(2)

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(Task(_, Unstarted | Waiting, 0),
                 Task(_, Unstarted, 0)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      destinations(1).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting, 0),
                 Task(_, Unstarted, 0)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))
      //We can't execute the following line because the previous reply will cause the
      //2nd command to start and thus destination(1) will receive a SimpleMessage
      //destinations(1).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting | Finished, 0),
                 Task(_, Unstarted | Waiting, 0)) => true
      }

      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])
      destinations(0).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished, 0),
                 Task(_, Waiting, 0)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))
      //We can't execute the following line because the previous reply will cause the
      //last command to finish and thus the orchestrator to finish which will make it
      //impossible for us to be asking its status
      //destinations(0).expectNoMsg(50.millis)

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished, 0),
                 Task(_, Finished, 0)) => true
      }
    }
  }

  test("Case 3: status of two parallel commands should be independent - Command0 replies first") {
    val destinations = Array.fill(2)(TestProbe())
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoCommand("Command0", destinations(0).ref.path, SimpleMessage)
      echoCommand("Command1", destinations(1).ref.path, SimpleMessage)
    }))

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(Task(_, Unstarted | Waiting, 0),
                 Task(_, Unstarted | Waiting, 0)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting, 0),
                 Task(_, Waiting, 0)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished, 0),
                 Task(_, Waiting, 0)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished, 0),
                 Task(_, Finished, 0)) => true
      }
    }
  }

  test("Case 4: status of two parallel commands should be independent - Command1 replies first") {
    val destinations = Array.fill(2)(TestProbe())
    val orchestrator = system.actorOf(Props(new StatelessOrchestrator {
      echoCommand("Command0", destinations(0).ref.path, SimpleMessage)
      echoCommand("Command1", destinations(1).ref.path, SimpleMessage)
    }))

    withOrchestratorTermination(orchestrator) { probe =>
      testStatus(orchestrator, probe) {
        case Seq(Task(_, Unstarted | Waiting, 0),
                 Task(_, Unstarted | Waiting, 0)) => true
      }

      val a0m = destinations(0).expectMsgClass(100.millis, classOf[SimpleMessage])
      val a1m = destinations(1).expectMsgClass(100.millis, classOf[SimpleMessage])

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting, 0),
                 Task(_, Waiting, 0)) => true
      }

      destinations(1).reply(SimpleMessage(a1m.id))

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Waiting, 0),
                 Task(_, Finished, 0)) => true
      }

      destinations(0).reply(SimpleMessage(a0m.id))

      testStatus(orchestrator, probe) {
        case Seq(Task(_, Finished, 0),
                 Task(_, Finished, 0)) => true
      }
    }
  }
}
