package pt.tecnico.dsi.akkastrator

import java.util.NoSuchElementException

import scala.concurrent.duration.Duration
import scala.collection.immutable.Seq
import pt.tecnico.dsi.akkastrator.HListConstraints.{TaskComapped, taskHListOps}
import pt.tecnico.dsi.akkastrator.Task._
import shapeless.{HList, HNil}

object FullTask {
  val taskColors = Vector(
    Console.MAGENTA,
    Console.CYAN,
    Console.GREEN,
    Console.BLUE,
    Console.YELLOW,
    Console.WHITE
  )
}

/**
  * @param description a text that describes this task in a human readable way. It will be used when the status of this
  *                    task orchestrator is requested.
  * @param dependencies the tasks that must have finished in order for this task to be able to start.
  * @param timeout NOTE: the timeout does not survive restarts!
  * @param createTask
  * @param orchestrator the orchestrator upon which this task will be added and ran. This is like an ExecutionContext for this task.
  * @param comapped
  * @tparam R the result type of this task.
  * @tparam DL the type of the dependencies HList.
  * @tparam RL the type of the results HList.
  */
final case class FullTask[R, DL <: HList, RL <: HList] (description: String, dependencies: DL, timeout: Duration = Duration.Inf,
                                                        createTask: RL => FullTask[R, DL, RL] => Task[R])
                                                       (implicit val orchestrator: AbstractOrchestrator[_], comapped: TaskComapped.Aux[DL, RL]) {
  /**
    * The index of this task in the task list maintained by the orchestrator.
    * It could also be called id since it uniquely identifies this task inside the corresponding orchestrator.
    */
  val index = orchestrator.addTask(this)
  
  lazy val color = FullTask.taskColors(index % FullTask.taskColors.size)
  def withColor(message: => String): String = {
    if (orchestrator.settings.useTaskColors) {
      s"$color$message${Console.RESET}"
    } else {
      message
    }
  }
  def withLogPrefix(message: => String): String = withColor(s"[$description] $message")
  
  // These fields are vars but once they are computed, they become "immutable", that is, they will no longer be modified.
  // The tasks that depend on this task. Used to notify them that this task has finished.
  private[this] var dependents = Seq.empty[FullTask[_, _, _]]
  // Used for the TaskReport. This way we just compute them once.
  private[this] var dependenciesIndexes = Seq.empty[Int]
  
  private def addDependent(dependent: FullTask[_, _, _]): Unit = dependents +:= dependent
  //Initialization
  dependencies.forEach { dependency =>
    dependency.addDependent(this)
    dependenciesIndexes +:= dependency.index
  }
  addToInitialTaskList()
  
  // These are the truly mutable state this class maintains
  private[this] var finishedDependencies = 0
  private[this] var innerTask = Option.empty[Task[R]]
  
  private def addToInitialTaskList(): Unit = {
    // By adding the tasks directly to the right list, we ensure that when the StartOrchestrator message
    // is received we do not need to iterate through all the tasks to compute which ones can start right away.
    if (dependencies == HNil) {
      orchestrator.waitingTasks += index -> innerCreateTask()
    } else {
      orchestrator.unstartedTasks += index -> this
    }
  }
  
  private def innerCreateTask(): Task[R] = {
    require(finishedDependencies == dependenciesIndexes.length,
      "Can only create the task when all of its dependencies have finished.")
    val resultsList = comapped.buildResultsList(dependencies)
    val task = createTask(resultsList)(this)
    innerTask = Some(task)
    task
  }
  
  private def dependencyFinished(): Unit = {
    finishedDependencies += 1
    if (finishedDependencies == dependenciesIndexes.length) {
      val task = innerCreateTask()
      task.start()
    }
  }
  
  /**
    * INTERNAL API
    * Iterates through the dependents of this tasks and informs them that this task has finished.
    * This is only called by Action.
    */
  private[akkastrator] def notifyDependents(): Unit = dependents.foreach(_.dependencyFinished())
  
  /**
    * INTERNAL API
    * This is only called by AbstractOrchestrator.
    */
  private[akkastrator] def restart(): Unit = {
    // Reset the mutable state of this Task
    finishedDependencies = 0
    innerTask = None
    // Re add this task to the correct list since the lists were cleared.
    addToInitialTaskList()
  }
  
  /** @return the current state of this task. */
  def state: Task.State = innerTask.map(_.state).getOrElse(Unstarted)
  
  /**
    * INTERNAL API
    * @return The result of this Task. A Task will only have a result if it is finished. */
  private[akkastrator] def result: R = state match {
    // You cannot see a Option.get here. You cannot see a Option.get here. You cannot see a Option.get here.
    case Finished(value) => value.asInstanceOf[R]
    case _ => throw new NoSuchElementException()
  }
  
  /** The immutable TaskReport of this task. */
  def toTaskReport: TaskReport = TaskReport(description, dependenciesIndexes, state)
  
  override def toString: String =
    f"""Task [$index%03d - $description]:
        |Destination: ${innerTask.map(_.destination.toString).getOrElse("Unavailable, since dependencies haven't finished.")}
        |State: $state""".stripMargin
}
