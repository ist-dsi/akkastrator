package pt.tecnico.dsi.akkastrator

import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration

import pt.tecnico.dsi.akkastrator.HListConstraints.{TaskComapped, taskHListOps}
import pt.tecnico.dsi.akkastrator.Orchestrator.StartTask
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.HList

// TODO: implement description in a much more awesome way (probably we will need scala.meta for it):
//  1) Define the description just once with something like:
//        d"Creating entry for user $userId" // userId is the result of a dependent task
//  2) When the task is unstarted its description will be: "Creating entry for user $userId"
//  3) When the task is finished its description will be: "Creating entry for user User(5, "Simão")"
//  4) Make this work with internationalization, aka, allow description to be used as a MessageKey for
//     a internationalization framework.
//  We could implement this with a function from hlist of options to string plus a list to be used for each of the orElses.
//  The string is a format string (to be used in String.format). By default this could simply be description
// abstract class FullTask[R, DL <: HList](val dependencies: DL, val timeout: Duration)(implicit val orchestrator: AbstractOrchestrator[_], val comapped: TaskComapped[DL]) {
//   def createTask(results: comapped.Results): Task[R]
//   def description(results: comapped.OptionResults, orElseValues: List[String]): String

/**
  * A full task represents a task plus its dependencies. It ensures a Task is only created when all of its dependencies
  * have finished.
  * @param description a text that describes this task in a human readable way, or a message key to be used in
  *                    internationalization. It will be used when the status of this task orchestrator is requested.
  * @param dependencies the tasks that must have finished in order for this task to be able to start.
  * @param timeout the timeout after which the task will be aborted. The timeout does not survive restarts.
  *                It should be `Duration.Inf` or a positive duration greater then zero.
  * @param orchestrator the orchestrator upon which this task will be added and ran. This is like an ExecutionContext for this task.
  * @param comapped evidence proving DL is a HList of FullTasks. Also allows us to extract the results from the dependencies.
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
  
  // The colors are a huge help when debugging an orchestrator however if the logs are being sent
  // to a file having colors makes the file troublesome to read
  lazy val color = orchestrator.settings.taskColors(index % orchestrator.settings.taskColors.size)
  def withColor(message: => String): String = {
    if (orchestrator.settings.useTaskColors) {
      s"$color$message${Console.RESET}"
    } else {
      message
    }
  }
  def withTaskPrefix(message: => String): String = withColor(s"[$description] $message")
  def withOrchestratorAndTaskPrefix(message: => String): String = orchestrator.withLogPrefix(withTaskPrefix(message))
  
  private def addDependent(dependent: FullTask[_, _]): Unit = dependents +:= dependent
  
  //Initialization
  dependencies.foreach { dependency =>
    dependency.addDependent(this)
    dependenciesIndexes +:= dependency.index
  }
  //From here on the dependents and dependenciesIndexes are no longer changed. They are just iterated over.
  
  def createTask(results: comapped.Results): Task[R]
  
  final def allDependenciesFinished: Boolean = finishedDependencies == dependenciesIndexes.length
  
  /**
    * Creates the `innerTask` using the results obtained from `buildResults` and returns it.
    * Also saves it in the innerTask field.
    */
  private[akkastrator] final def innerCreateTask(): Task[R] = {
    require(allDependenciesFinished, "All dependencies must have finished to be able to build their results.")
    val task = createTask(comapped.results(dependencies))
    innerTask = Some(task)
    task
  }
  
  /** Starts this task. */
  private[akkastrator] final def start(): Unit = innerCreateTask().start()
  
  /** Iterates through the dependents of this task and informs them that this task has finished. */
  private[akkastrator] final def notifyDependents(): Unit = dependents.foreach(_.dependencyFinished())
  
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
        orchestrator.self ! StartTask(index)
      }
      // When the orchestrator is recovering we do not send the StartTask for the following reasons:
      //  · The TaskStarted would always be handled before the StartTask in the receiveRecover of the orchestrator.
      //  · The recover of TaskStarted already invokes fulltask.start, which is the side-effect of handling the StartTask message
      //    So if we also sent the StartTask then the task would be started again at a later time when it was already waiting.
    }
  }
  
  /** @return the current state of this task. */
  final def state: Task.State = innerTask.map(_.state).getOrElse(Unstarted)
  
  final def result: Option[R] = innerTask.map(_.state).collect { case Finished(result) => result.asInstanceOf[R] }
  
  /**
    * Returns this task result if it already finished. Otherwise throws an exception.
    */
  final def unsafeResult: R = {
    // At least fail with a better message
    result.getOrElse(throw new IllegalStateException("A task only has a result if it finished."))
  }

  /** The TaskReport of this task. */
  final def report: Report[R] = Report(index, description, dependenciesIndexes, state,
    innerTask.map(_.destination), result)
  
  override def toString: String =
    f"""FullTask [$index%03d - $description]:
        |Destination: ${innerTask.map(_.destination.toString).getOrElse("Unavailable, since dependencies haven't finished.")}
        |State: $state""".stripMargin //State will contain the result
}
