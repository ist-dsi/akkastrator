package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import akka.actor.Props
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import shapeless.ops.hlist.{At, Tupler}
import shapeless.{::, =:!=, Generic, HList, HNil, _0}

// TODO: the interplay of names between FullTask and Task is bad.
//  1) Maybe we should do something like impromptu Task.after
//  2) Or WithDependenciesTask and NoDependenciesTask
//  3) Or WithDependencies and NoDependencies

object DSL {
  type TaskBuilder[R] = FullTask[R, _] => Task[R]
  
  object FullTask {
    class PartialTask[DL <: HList, RL <: HList, RP] (description: String, dependencies: DL, timeout: Duration)
                                                    (implicit cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]) {
      // Ideally all of these methods would be called `createTask` but due to type erasure we cannot declare them so.
      def createTaskWith[R](f: RL => TaskBuilder[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, DL] = {
        new FullTask[R, DL](description, dependencies, timeout)(orchestrator, cm) {
          def createTask(results: comapped.Results): Task[R] = f(results.asInstanceOf[RL])(this)
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
    // We need this apply in order for it to win over the (description: String, dependencies: DP)
    /**
      * Allows creating a FullTask using HList syntax:
      * {{{
      *   FullTask("some description", a :: b :: HNil) createTaskWith { case aResult :: bResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DL <: HList, RL <: HList, RP](description: String, dependencies: DL)
                                           (implicit cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]): PartialTask[DL, RL, RP] = {
      new PartialTask(description, dependencies, Duration.Inf)
    }
  
    /**
      * Allows creating a FullTask using tuple syntax with a timeout:
      * {{{
      *   FullTask("some description", (a, b), Duration.Inf) createTaskWith { case aResult :: bResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DP, DL <: HList, RL <: HList, RP](description: String, dependencies: DP, timeout: Duration)
                                               (implicit ev: DP =:!= HList, gen: Generic.Aux[DP, DL],
                                                cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]): PartialTask[DL, RL, RP] = {
      implicitly[DP =:!= HList] // NOP to ensure ev is used and -Ywarn-unused:params does not trip
      new PartialTask(description, gen.to(dependencies), timeout)
    }
    /**
      * Allows creating a FullTask using tuple syntax:
      * {{{
      *   FullTask("some description", (a, b)) createTaskWith { case aResult :: bResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DP, DL <: HList, RL <: HList, RP](description: String, dependencies: DP)
                                               (implicit ev: DP =:!= HList, gen: Generic.Aux[DP, DL],
                                                cm: TaskComapped.Aux[DL, RL], tupler: Tupler.Aux[RL, RP]): PartialTask[DL, RL, RP] = {
      implicitly[DP =:!= HList] // NOP to ensure ev is used and -Ywarn-unused:params does not trip
      new PartialTask(description, gen.to(dependencies), Duration.Inf)
    }
    
    /**
      * Simplifies the creation of a FullTask with a single dependency and a timeout:
      * {{{
      *   FullTask("some description", a, Duration.Inf) createTaskWith { case aResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DR, DDL <: HList](description: String, dependency: FullTask[DR, DDL], timeout: Duration): PartialTask[FullTask[DR, DDL] :: HNil, DR :: HNil, Tuple1[DR]] = {
      new PartialTask(description, dependency :: HNil, timeout)
    }
    /**
      * Simplifies the creation of a FullTask with a single dependency:
      * {{{
      *   FullTask("some description", a) createTaskWith { case aResult :: HNil =>
      *     // Task creation
      *   }
      * }}}
      */
    def apply[DR, DDL <: HList](description: String, dependency: FullTask[DR, DDL]): PartialTask[FullTask[DR, DDL] :: HNil, DR :: HNil, Tuple1[DR]] = {
      new PartialTask(description, dependency :: HNil, Duration.Inf)
    }
  }
  
  object TaskSpawnOrchestrator {
    def apply[R, O <: AbstractOrchestrator[R]: ClassTag](props: Props): TaskBuilder[R] = {
      new TaskSpawnOrchestrator[R, O](_)(props)
    }
  }
  object TaskBundle {
    // tasksCreator should be of type `implicit AbstractOrchestrator[_] => Seq[FullTask[R, HNil]]`
    // but unfortunately only Dotty has implicit function types)
    def apply[R](tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]]): TaskBuilder[Seq[R]] = {
      new TaskBundle(_)(tasksCreator)
    }
  }
  object TaskQuorum {
    // tasksCreator should be of type `implicit AbstractOrchestrator[_] => Seq[FullTask[R, HNil]]`
    // but unfortunately only Dotty has implicit function types)
    def apply[R](minimumVotes: MinimumVotes)(tasksCreator: AbstractOrchestrator[_] => Seq[FullTask[R, HNil]]): TaskBuilder[R] = {
      new TaskQuorum(_, minimumVotes)(tasksCreator)
    }
  }
  
  // Allows dependency isDependencyOf otherTaskMethod
  implicit class richFullTask[DR, DDL <: HList](val dependency: FullTask[DR, DDL]) extends AnyVal {
    def isDependencyOf[R](task: FullTask[DR, DDL] :: HNil => FullTask[R, _]): FullTask[R, _] = {
      task(dependency :: HNil)
    }
  }

  // Allows task1 :: task2 :: HNil areDependenciesOf otherTaskMethod, where otherTaskMethod:
  //  def otherTaskMethod(dependencies: FullTask[String, _] :: FullTask[Unit, _] :: HNil)
  implicit class hlist2TaskWithDependencies[DL <: HList](dependencies: DL)
                                                        (implicit ev: TaskComapped[DL], at: At[DL, _0]) {
    def areDependenciesOf[R](task: DL => FullTask[R, _]): FullTask[R, _] = task(dependencies)
  }

  // Allows (task1, task2) areDependenciesOf otherTaskMethod
  implicit def tuple2TaskWithDependencies[DL <: HList, DP <: Product](dependencies: DP)
                                                                     (implicit gen: Generic.Aux[DP, DL],
                                                                      ev: TaskComapped[DL],
                                                                      at: At[DL, _0]): hlist2TaskWithDependencies[DL] = {
    new hlist2TaskWithDependencies[DL](gen.to(dependencies))
  }
}
