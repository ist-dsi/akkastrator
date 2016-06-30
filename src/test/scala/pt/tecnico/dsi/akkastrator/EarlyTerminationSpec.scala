package pt.tecnico.dsi.akkastrator

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class EarlyTerminationSpec extends ActorSysSpec with FunSuiteLike with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  //Ensure the following happens:
  //  · This task will be finished.
  //  · Every unstarted task will be prevented from starting even if its dependencies have finished.
  //  · Tasks that are waiting will remain untouched and the orchestrator will
  //    still be prepared to handle their responses.
  //  · The method `onEarlyTermination` will be invoked in the orchestrator.
}
