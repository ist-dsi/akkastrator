package pt.tecnico.dsi.akkastrator

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class SnapshotsSpec extends ActorSysSpec with FunSuiteLike with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  //Test:
  // · saveSnapshotEveryXMessages = 0
  // · saveSnapshotEveryXMessages != 0
}
