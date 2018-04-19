package pt.tecnico.dsi.akkastrator

import com.typesafe.config.{Config, ConfigFactory}

object Settings {
  /**
    * Instantiate a `Settings` from a `Config`.
    *
    * @param config The `Config` from which to parse the settings.
    */
  def fromConfig(config: Config = ConfigFactory.load()): Settings = {
    val akkastratorConfig: Config = {
      val reference = ConfigFactory.defaultReference()
      val finalConfig = config.withFallback(reference)
      finalConfig.checkValid(reference, "akkastrator")
      finalConfig.getConfig("akkastrator")
    }
    import akkastratorConfig._
    
    Settings(getBoolean("use-task-colors"))
  }
}

/**
  * This class holds all the settings that parameterize akkastrator.
  *
  * If you would like to create an instance of settings from a typesafe config invoke `Settings.fromConfig`.
  *
  * @param useTaskColors
  * @param taskColors
  */
final case class Settings(useTaskColors: Boolean = true, taskColors: Seq[String] = Vector(
  Console.MAGENTA,
  Console.CYAN,
  Console.GREEN,
  Console.BLUE,
  Console.YELLOW,
  Console.WHITE
))