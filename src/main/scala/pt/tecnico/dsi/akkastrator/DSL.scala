package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration

import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped
import shapeless.ops.hlist.At
import shapeless.{::, Generic, HList, HNil, _0}

object DSL {
  object FullTask {
    class PartialTask[DL <: HList, RL <: HList] private[FullTask] (description: String, dependencies: DL, timeout: Duration)
                                                                  (implicit cm: TaskComapped.Aux[DL, RL]) {
      /*
      // Because of type erasure we cannot declare this method :(
      def createTaskWith[R](f: FullTask[_, _] => Task[R])(implicit orchestrator: AbstractOrchestrator[_], ev: RL =:= HNil): FullTask[R, DL] = {
        new FullTask[R, DL](description, dependencies, timeout)(orchestrator, cm) {
          def createTask(results: comapped.ResultsList): Task[R] = f(this)
        }
      }*/
      def createTaskWith[R](f: RL => FullTask[_, _] => Task[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, DL] = {
        new FullTask[R, DL](description, dependencies, timeout)(orchestrator, cm) {
          def createTask(results: comapped.ResultsList): Task[R] = f(results.asInstanceOf[RL])(this)
        }
      }
      /*//Yey for type erasure </sarcasm>
      def createTaskWithTuple[R](f: RP => FullTask[_, _] => Task[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, DL] = {
        createTaskWith(resultsList => f(tupler(resultsList)))
      }*/
      
      /*
      import shapeless.ops.function.FnToProduct
      def createTask[F, T, R](builder: F)(implicit orchestrator: AbstractOrchestrator[_],
                                          fntp: FnToProduct.Aux[F, RL => T],
                                          ev: <:<[T, FullTask[_, _] => Task[R]]): FullTask[R, DL] = {
        createTaskWith(fntp(builder) andThen ev)
      }
      //TODO: does Predef.ArrowAssoc takes precedence over this one?
      def ->[F, T, R](builder: F)(implicit orchestrator: AbstractOrchestrator[_], fntp: FnToProduct.Aux[F, RL => T],
                                  ev: <:<[T, FullTask[_, _] => Task[R]]): FullTask[R, DL] = {
        createTask(builder)
      }
      */
    }
  
    /**
      * Allows creating a FullTask using tuple syntax:
      * {{{
      *   FullTask("some description", (a, b), Duration.Inf) createTaskWith { case aResult :: bResult :: HNil =>
      *
      *   }
      * }}}
      */
    def apply[DP <: Product, DL <: HList, RL <: HList](description: String, dependencies: DP, timeout: Duration)
                                                      (implicit gen: Generic.Aux[DP, DL], cm: TaskComapped.Aux[DL, RL]): PartialTask[DL, RL] = {
      new PartialTask(description, gen.to(dependencies), timeout)
    }
  
    /**
      * Allows creating a FullTask using HList syntax:
      * {{{
      *   FullTask("some description", a :: b :: HNil) createTaskWith { case aResult :: bResult :: HNil =>
      *
      *   }
      * }}}
      */
    def apply[DL <: HList, RL <: HList](description: String, dependencies: DL = HNil: HNil, timeout: Duration = Duration.Inf)
                                       (implicit cm: TaskComapped.Aux[DL, RL]): PartialTask[DL, RL] = {
      new PartialTask(description, dependencies, timeout)
    }
  }
  
  // The DSL for FullTask returns a FullTask[R, DL] (this is needed for TaskSpawnOrchestrator and derivatives, because they expect FullTask[R, HNil])
  // whereas the DSL for dependencies returns FullTask[R, _ <: HList]
  // This means that when using the dependencies DSL we do not allow the following:
  // val a: FullTask[String, HNil] = //...
  // val b: FullTask[Int, HNil] = //...
  // def changePassword(dependencies: FullTask[String, HNil] :: FullTask[String, HNil] :: HNil) = {
  //   FullTask("changePassword", dependencies) createTaskWith { case what :: where :: HNil =>
  //     new Task[Unit](_) {
  //       //...
  //     }}
  //   }
  // }
  // val c = (a, b) areDependenciesOf changePassword //Works and c is of type FullTask[Unit, _ <: HList]
  // def doSomething(dependencies: FullTask[String, HNil] :: FullTask[Unit, HNil] :: HNil) = //...
  // (a, c) areDependenciesOf doSomething //Does not work since c is not a FullTask[Unit, HNil]
  //
  // This is not very problematic since we don't really care about the dependencies of a task except for the HNil dependency
  
  
  // Allows dependency isDependencyOf otherTaskMethod
  implicit class richFullTask[DR](val dependency: FullTask[DR, _ <: HList]) extends AnyVal {
    def isDependencyOf[R](task: FullTask[DR, _] :: HNil => FullTask[R, _ <: HList]): FullTask[R, _ <: HList] = {
      task(dependency :: HNil)
    }
    def ->[R](task: FullTask[DR, _] :: HNil => FullTask[R, _ <: HList]): FullTask[R, _ <: HList] = {
      isDependencyOf(task)
    }
  }
  
  // Allows (task1, task2) areDependenciesOf otherTaskMethod
  implicit def tuple2TaskWithDependencies[DL <: HList, DP <: Product](dependencies: DP)
                                                                     (implicit gen: Generic.Aux[DP, DL],
                                                                      ev: TaskComapped[DL],
                                                                      at: At[DL, _0]): hlist2TaskWithDependencies[DL] = {
    new hlist2TaskWithDependencies[DL](gen.to(dependencies))
  }
  
  // Allows task1 :: task2 :: HNil areDependenciesOf otherTaskMethod, where otherTaskMethod:
  //  def otherTaskMethod(dependencies: FullTask[String, _] :: FullTask[Unit, _] :: HNil)
  implicit class hlist2TaskWithDependencies[DL <: HList](dependencies: DL)(implicit ev: TaskComapped[DL], at: At[DL, _0]) {
    def areDependenciesOf[R](task: DL => FullTask[R, _ <: HList]): FullTask[R, _ <: HList] = task(dependencies)
    def ->[R](task: DL => FullTask[R, _ <: HList]): FullTask[R, _ <: HList] = areDependenciesOf(task)
  }
}
