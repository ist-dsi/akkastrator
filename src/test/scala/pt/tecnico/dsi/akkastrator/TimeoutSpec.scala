package pt.tecnico.dsi.akkastrator

class TimeoutSpec extends ActorSysSpec {
  //Test:
  // Timeout = Duration.Inf => does not cause any timeout
  // Timeout = FiniteDuration causes a timeout, sending a Task.Timeout to the task behavior.
  //  · If the task handles that message, check it is correctly handled
  //  · If the task does not handle that message then check the task aborts with cause = TimedOut
}
