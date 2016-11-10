package pt.tecnico.dsi.akkastrator

class Step9_RestartSpec extends ActorSysSpec {
  //Test:
  // · Restarting an orchestrator without inner orchestrators
  //    · Especially in a distinctIds orchestrator to ensure the DistinctIds state is correctly reseted
  // · With inner orchestrators but they already finished
  // · With inner orchestrators when they were halfway of doing their job
}
