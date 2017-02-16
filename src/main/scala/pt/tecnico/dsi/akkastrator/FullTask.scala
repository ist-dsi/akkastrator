package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration
import scala.collection.immutable.Seq

import pt.tecnico.dsi.akkastrator.HListConstraints.{TaskComapped, taskHListOps}
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.{HList, HNil}

/**
  * @param description a text that describes this task in a human readable way, or a message key to be used in
  *                    internationalization. It will be used when the status of this task orchestrator is requested.
  * @param dependencies the tasks that must have finished in order for this task to be able to start.
  * @param timeout NOTE: the timeout does not survive restarts!
  * @param orchestrator the orchestrator upon which this task will be added and ran. This is like an ExecutionContext for this task.
  * @param comapped
  * @tparam R the result type of this task.
  * @tparam DL the type of the dependencies HList.
  */
abstract class FullTask[R, DL <: HList](val description: String, val dependencies: DL, val timeout: Duration)
                                       (implicit val orchestrator: AbstractOrchestrator[_], val comapped: TaskComapped[DL]) {
  // These fields are vars but once they are computed, they become "immutable", that is, they will no longer be modified.
  // Dependents = tasks that depend on this task. Used to notify them that this task has finished.
  private[this] final var dependents = Seq.empty[FullTask[_, _]]
  // Used for the TaskReport. This way we just compute the task report dependencies once.
  private[this] final var dependenciesIndexes = Seq.empty[Int]
  
  // These are the truly mutable state this class maintains
  private[this] final var finishedDependencies = 0
  private[this] final var innerTask = Option.empty[Task[R]]
  
  // This is both an artifact that greatly simplifies the code, but also makes it very tightly coupled.
  /**
    * The index of this task in the task list maintained by the orchestrator.
    * It could also be called id since it uniquely identifies this task inside the corresponding orchestrator.
    */
  final val index = orchestrator.addTask(this)
  
  lazy val color = orchestrator.settings.taskColors(index % orchestrator.settings.taskColors.size)
  def withColor(message: => String): String = {
    if (orchestrator.settings.useTaskColors) {
      s"$color$message${Console.RESET}"
    } else {
      message
    }
  }
  def withLogPrefix(message: => String): String = withColor(s"[$description] $message")
  
  private def addDependent(dependent: FullTask[_, _]): Unit = dependents +:= dependent
  
  //Initialization
  dependencies.forEach { dependency =>
    dependency.addDependent(this)
    dependenciesIndexes +:= dependency.index
  }
  //From here on the dependents and dependenciesIndexes are no longer changed. They are just iterated over.
  
  // By adding the tasks directly to the right list, we ensure that when the StartOrchestrator message
  // is received we do not need to iterate through all the tasks to compute which ones can start right away.
  if (dependencies == HNil) {
    orchestrator.waitingTasks += index -> innerCreateTask()
  } else {
    orchestrator.unstartedTasks += index -> this
  }
  
  def createTask(results: comapped.ResultsList): Task[R]
  
  /*
  def createFormattedDescription(results: RL): String = {
    // This is the default implementation
    messageKey
    // The more realistic implementation should be:
  
    //val pattern = fetch the pattern from a message file using description as a messageKey
    //MessageFormat.format(pattern, results.toList[Any]:_*)
  }
  
  def description: String = {
    if (allDependenciesFinished) {
      createFormattedDescription(buildResults())
    } else {
      messageKey
    }
  }
  */
  
  protected final def allDependenciesFinished: Boolean = finishedDependencies == dependenciesIndexes.length
  
  /** Computes the results HList from the dependencies HList.
    * Every dependency must have finished, otherwise an exception will be thrown. */
  protected final def buildResults(): comapped.ResultsList = {
    require(allDependenciesFinished, "All dependencies must have finished to be able to build their results.")
    comapped.buildResultsList(dependencies)
  }
  
  /**
    * Creates the `innerTask` using the results obtained from `buildResults` and returns it.
    * Also saves it in the innerTask field.
    */
  private final def innerCreateTask(): Task[R] = {
    val results = buildResults()
    val task = createTask(results)
    innerTask = Some(task)
    task
  }
  
  /** When a dependency of this task finishes this method is invoked. */
  private final def dependencyFinished(): Unit = {
    finishedDependencies += 1
    if (allDependenciesFinished) {
      // This method is being invoked from inside the persist handler of innerTask.finish
      // By sending a message to the orchestrator (instead of starting the task right away) we break out of
      // the persist handler. This is advantageous since:
      //  · While in the persist handler messages that are sent to the orchestrator are being stashed,
      //    if we started the task directly we would never give a change for the orchestrator to handle
      //    status, shutdown, etc messages.
      //  · The persist handle will be executing during a much shorter period of time.
      // This also means that tasks are started in a breadth-first order.
      if (!orchestrator.recoveryRunning) {
        orchestrator.self ! orchestrator.StartTask(index)
      }
      // When the orchestrator is recovering we do not send the StartTask for the following reasons:
      //  · The MessageSent would always be handled before the StartTask in the receiveRecover of the orchestrator.
      //  · The recover of MessageSent already invokes fulltask.start, which is the side-effect of handling the StartTask message
      //    So if we also sent the StartTask then the task would be started again at a later time when it was already waiting.
    }
  }
  
  private[akkastrator] final def start(): Unit = innerCreateTask().start()
  
  /** Iterates through the dependents of this task and informs them that this task has finished. */
  final def notifyDependents(): Unit = dependents.foreach(_.dependencyFinished())
  
  /** @return the current state of this task. */
  final def state: Task.State = innerTask.map(_.state).getOrElse(Unstarted)
  
  final def result: Option[R] = innerTask.map(_.state).collect { case Finished(result) => result.asInstanceOf[R] }
  
  /**
    * Returns this task result if it already finished. Otherwise throws an exception.
    */
  final def unsafeResult: R = result match {
    case Some(result) => result
    case None => throw new IllegalStateException("A task only has a result if it finished.")
  }
  
  /** The immutable TaskReport of this task. */
  final def report: Report[R] = Report(description, dependenciesIndexes, state,
    innerTask.map(_.destination), result)
  
  override def toString: String =
    f"""Task [$index%03d - $description]:
        |Destination: ${innerTask.map(_.destination.toString).getOrElse("Unavailable, since dependencies haven't finished.")}
        |State: $state""".stripMargin //State will contain the result
}
