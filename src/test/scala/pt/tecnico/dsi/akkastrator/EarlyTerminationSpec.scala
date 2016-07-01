package pt.tecnico.dsi.akkastrator

class EarlyTerminationSpec extends ActorSysSpec {
  //Ensure the following happens:
  //  · This task will be finished.
  //  · Every unstarted task will be prevented from starting even if its dependencies have finished.
  //  · Tasks that are waiting will remain untouched and the orchestrator will
  //    still be prepared to handle their responses.
  //  · The method `onEarlyTermination` will be invoked in the orchestrator.

  /**
    * This will cause this task orchestrator to terminate early.
    * An early termination will have the following effects:
    *   · This task will be finished.
    *   · Every unstarted task will be prevented from starting
    *   · Tasks that are waiting will remain untouched.
    *   · The method `onEarlyTermination` will be invoked in the orchestrator.
    *   · The method `onFinish` will NEVER be called.
    */
}
