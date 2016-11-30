package pt.tecnico.dsi.akkastrator

import com.typesafe.config.{Config, ConfigFactory}

/**
  * This class holds all the settings that parameterize akkastrator.
  *
  * By default these settings are read from the Config obtained with `ConfigFactory.load()`.
  *
  * You can change the settings in multiple ways:
  *
  *  - Change them in the default configuration file (e.g. application.conf)
  *  - Pass a different config holding your configurations: {{{
  *       new Settings(yourConfig)
  *     }}}
  *     However it will be more succinct to pass your config directly to your akkastrator: {{{
  *      context.actorOf(Props(classOf[YourOrchestrator], yourConfig))
  *     }}}
  *  - Extend this class overriding the settings you want to redefine {{{
  *      object YourSettings extends Settings() {
  *        override val saveSnapshotEveryXMessages = 1000
  *      }
  *      context.actorOf(Props(classOf[YourOrchestrator], YourSettings))
  *    }}}
  *
  * @param config
  */
class Settings(config: Config = ConfigFactory.load()) {
  val akkastratorConfig: Config = {
    val reference = ConfigFactory.defaultReference()
    val finalConfig = config.withFallback(reference)
    finalConfig.checkValid(reference, "akkastrator")
    finalConfig.getConfig("akkastrator")
  }
  import akkastratorConfig._

  val saveSnapshotRoughlyEveryXMessages: Int = getInt("save-snapshot-roughly-every-X-messages")
  val useTaskColors: Boolean = getBoolean("use-task-colors")
  
  val taskColors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.WHITE
  )

  override def toString: String = akkastratorConfig.root.render
}

