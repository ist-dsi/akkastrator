package pt.tecnico.dsi.akkastrator

import akka.actor.Props
import akka.testkit.{EventFilter, TestProbe}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import pt.tecnico.dsi.akkastrator.DSL.FullTask
import pt.tecnico.dsi.akkastrator.Orchestrator.StartOrchestrator
import pt.tecnico.dsi.akkastrator.Step0_SettingsSpec.{WithColors, WithoutColors}

import scala.collection.JavaConverters._

object Step0_SettingsSpec {
  class WithColors(probe: TestProbe) extends TaskColorsOrchestrator(probe, Settings(useTaskColors = true))
  class WithoutColors(probe: TestProbe) extends TaskColorsOrchestrator(probe, Settings(useTaskColors = false))
  class TaskColorsOrchestrator(probe: TestProbe, settings: Settings) extends Orchestrator[Unit](settings) {
    override def persistenceId: String = this.getClass.getSimpleName

    FullTask("A") createTask[String] { _ =>
      new Task[String](_) {
        override val destination = probe.ref.path
        override def createMessage(id: Long) = SimpleMessage(id)
        override def behavior = {
          case SimpleMessage(id) if matchId(id) => finish("Finished")
        }
      }
    }
  }
}

class Step0_SettingsSpec extends ActorSysSpec(Some(ConfigFactory.load().withValue("akka.loggers", ConfigValueFactory.fromIterable(List("akka.testkit.TestEventListener").asJava)))) {
  "Settings created from TypeSafe config" should {
    "have the same default values as the domain class" in {
      Settings.fromConfig() shouldBe new Settings()
    }
  }

  val testProbe = TestProbe("A")

  "When useTaskColors = false the log messages" should {
    "not have console colors" in {
      val orchestrator = system.actorOf(Props(classOf[WithoutColors], testProbe))
      orchestrator ! StartOrchestrator(1L)

      EventFilter.info(pattern = " \\[A\\] Finishing", occurrences = 1) intercept {
        val m = testProbe.expectMsgType[SimpleMessage]
        testProbe reply m
      }
    }
  }

  "When useTaskColors = true the log messages" should {
    "have console colors" in {
      val orchestrator = system.actorOf(Props(classOf[WithColors], testProbe))
      orchestrator ! StartOrchestrator(1L)

      EventFilter.info(pattern = " \u001b\\[35m\\[A\\] Finishing", occurrences = 1) intercept {
        val m = testProbe.expectMsgType[SimpleMessage]
        testProbe reply m
      }
    }
  }
}
