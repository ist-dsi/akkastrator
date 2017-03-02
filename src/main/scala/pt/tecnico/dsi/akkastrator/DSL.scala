package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration

import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import shapeless.ops.hlist.{At, Tupler}
import shapeless.{Generic, HList, HNil, _0, ::}

object DSL {
  type TaskBuilder[R] = FullTask[_, _] => Task[R]
  
  object FullTask {
    class PartialTask[DL <: HList, RL <: HList, RP] private[FullTask] (description: String, dependencies: DL, timeout: Duration)
                                                                      (implicit cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]) {
      // Ideally all of these methods would be called `createTask` but due to type erasure we cannot declare them so.
      
      def createTaskWith[R](f: RL => TaskBuilder[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, DL] = {
        new FullTask[R, DL](description, dependencies, timeout)(orchestrator, cm) {
          def createTask(results: comapped.ResultsList): Task[R] = f(results.asInstanceOf[RL])(this)
        }
      }
      def createTask[R](f: RP => TaskBuilder[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, DL] = {
        createTaskWith(resultsList => f(tupler(resultsList)))
      }
      
      import shapeless.ops.function.FnToProduct
      def createTaskF[F, T, R](builder: F)(implicit orchestrator: AbstractOrchestrator[_], fntp: FnToProduct.Aux[F, RL => T],
                                          ev: T <:< TaskBuilder[R]): FullTask[R, DL] = {
        createTaskWith(fntp(builder) andThen ev)
      }
    }
  
    /**
      * Simplifies the creation of a FullTask with a single dependency:
      * {{{
      *   FullTask("some description", a, Duration.Inf) createTaskWith { case aResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DR, DDL <: HList](description: String, dependency: FullTask[DR, DDL],
                                timeout: Duration): PartialTask[FullTask[DR, DDL] :: HNil, DR :: HNil, Tuple1[DR]] = {
      new PartialTask(description, dependency :: HNil, timeout)
    }
    /**
      * Allows creating a FullTask using tuple syntax:
      * {{{
      *   FullTask("some description", (a, b), Duration.Inf) createTaskWith { case aResult :: bResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DP, DL <: HList, RL <: HList, RP](description: String, dependencies: DP, timeout: Duration)
                                               (implicit gen: Generic.Aux[DP, DL], cm: TaskComapped.Aux[DL, RL],
                                                tupler: Tupler.Aux[RL, RP]): PartialTask[DL, RL, RP] = {
      new PartialTask(description, gen.to(dependencies), timeout)
    }
    /**
      * Allows creating a FullTask using HList syntax:
      * {{{
      *   FullTask("some description", a :: b :: HNil) createTaskWith { case aResult :: bResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DL <: HList, RL <: HList, RP](description: String, dependencies: DL = HNil: HNil, timeout: Duration = Duration.Inf)
                                           (implicit cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]): PartialTask[DL, RL, RP] = {
      new PartialTask(description, dependencies, timeout)
    }
  }
  
  
  // Allows dependency isDependencyOf otherTaskMethod
  implicit class richFullTask[DR, DDL <: HList](val dependency: FullTask[DR, DDL]) extends AnyVal {
    def isDependencyOf[R](task: FullTask[DR, DDL] :: HNil => FullTask[R, _]): FullTask[R, _] = {
      task(dependency :: HNil)
    }
  
    // REVIEW: this method might lead to some confusion due to any2ArrowAssoc
    def ->[R](task: FullTask[DR, DDL] :: HNil => FullTask[R, _]): FullTask[R, _] = {
      isDependencyOf(task)
    }
  }

  // Allows task1 :: task2 :: HNil areDependenciesOf otherTaskMethod, where otherTaskMethod:
  //  def otherTaskMethod(dependencies: FullTask[String, _] :: FullTask[Unit, _] :: HNil)
  implicit class hlist2TaskWithDependencies[DL <: HList](dependencies: DL)
                                                        (implicit ev: TaskComapped[DL], at: At[DL, _0]) {
    def areDependenciesOf[R](task: DL => FullTask[R, _]): FullTask[R, _] = task(dependencies)
  
    // REVIEW: this method might lead to some confusion due to any2ArrowAssoc
    def ->[R](task: DL => FullTask[R, _]): FullTask[R, _] = areDependenciesOf(task)
  }

  // Allows (task1, task2) areDependenciesOf otherTaskMethod
  implicit def tuple2TaskWithDependencies[DL <: HList, DP <: Product](dependencies: DP)
                                                                     (implicit gen: Generic.Aux[DP, DL],
                                                                      ev: TaskComapped[DL],
                                                                      at: At[DL, _0]): hlist2TaskWithDependencies[DL] = {
    new hlist2TaskWithDependencies[DL](gen.to(dependencies))
  }
}
