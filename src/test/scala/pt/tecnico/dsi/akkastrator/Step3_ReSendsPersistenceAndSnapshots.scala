package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}


class Step3_ReSendsPersistenceAndSnapshots extends TestKit(ActorSystem("Orchestrator", ConfigFactory.load()))
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with TestUtils {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

}
