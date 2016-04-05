package pt.tecnico.dsi.akkastrator

import akka.actor._
import akka.persistence.Recovery
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}


class Step2_RetryAndStatusSpec extends TestKit(ActorSystem("Orchestrator", ConfigFactory.load()))
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with TestUtils {

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

}
