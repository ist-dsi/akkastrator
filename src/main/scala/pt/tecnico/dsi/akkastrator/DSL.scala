package pt.tecnico.dsi.akkastrator

import scala.concurrent.duration.Duration

import akka.actor.{Actor, ActorPath}
import shapeless.{HList, HNil}
import shapeless.Generic
import shapeless.ops.hlist.Tupler
import shapeless._0
import shapeless.ops.hlist.At
import pt.tecnico.dsi.akkastrator.HListConstraints.TaskComapped

object DSL {
  //The FullTask DSL is possible due to these classes
  //TODO: would using @inline in these methods help?
  //TODO: how to deal with timeouts?
  
  implicit class string2FullTask(val description: String) extends AnyVal {
    //Supports the creation of a FullTask where the inner task has no dependencies
    def describes[R](task: FullTask[_, HNil, HNil] => Task[R])(implicit orchestrator: AbstractOrchestrator[_]): FullTask[R, HNil, HNil] = {
      FullTask(description, HNil, createTask = _ => task)
    }
    //Supports the creation of a FullTask where the inner task has dependencies
    def describes[R, DL <: HList, RL <: HList](task: TaskWithDependencies[R, DL, RL]): FullTask[R, DL, RL] = {
      task(description)
    }
  }
  
  //How do you get around type erasure? Simple, you "waste" memory creating a TaskWithDependencies class.
  abstract class TaskWithDependencies[R, DL <: HList, RL <: HList] extends (String => FullTask[R, DL, RL])
  
  implicit def givenDependencies[DL <: HList, DP <: Product, RL <: HList, RP <: Product](dependencies: DP)
                                                                                        (implicit orchestrator: AbstractOrchestrator[_],
                                                                                         gen: Generic.Aux[DP, DL],
                                                                                         ev: TaskComapped.Aux[DL, RL],
                                                                                         t: Tupler.Aux[RL, RP],
                                                                                         at: At[DL, _0]): givenDependencies[DL, RL, RP] = {
    new givenDependencies(gen.to(dependencies))
  }
  implicit class givenDependencies[DL <: HList, RL <: HList, RP <: Product](val dependencies: DL)
                                                                           (implicit orchestrator: AbstractOrchestrator[_],
                                                                            ev: TaskComapped.Aux[DL, RL],
                                                                            tupler: Tupler.Aux[RL, RP],
                                                                            at: At[DL, _0]) {
    def createTaskWith[R](builder: RL => FullTask[_, DL, RL] => Task[R]): TaskWithDependencies[R, DL, RL] = {
      new TaskWithDependencies[R, DL, RL] {
        def apply(description: String): FullTask[R, DL, RL] = FullTask(description, dependencies, createTask = builder)
      }
    }
    def createTaskWithTuple[R](builder: RP => FullTask[_, DL, RL] => Task[R]): TaskWithDependencies[R, DL, RL] = {
      createTaskWith(resultsList => builder(tupler(resultsList)))
    }
  
    import shapeless.ops.function.FnToProduct
    def createTask[F, T, R](builder: F)(implicit fntp: FnToProduct.Aux[F, RL => T], ev: <:<[T, FullTask[_, DL, RL] => Task[R]]): TaskWithDependencies[R, DL, RL] = {
      createTaskWith(fntp(builder) andThen ev)
    }
    def ->[F, T, R](builder: F)(implicit fntp: FnToProduct.Aux[F, RL => T], ev: <:<[T, FullTask[_, DL, RL] => Task[R]]): TaskWithDependencies[R, DL, RL] = {
      createTaskWith(fntp(builder) andThen ev)
    }
  }
  
  /*kerberos !! (Kerberos.addPrincipal("", _)) withBehavior {
    case m @ Success(id) if matchId(id) =>
  }*/
  /*kerberos deliver (AddPrincipal("", _)) withBehavior {
    case m @ AddPrincipal(_, _) => println("bla")
  }*/
  /*
  TODO: where to specify the type parameter R?
  implicit class actorPath2Task(path: ActorPath) {
    class taskBehavior(f: Long => Any) {
      def withBehavior(behavior: Actor.Receive): Task[?] = new Task[?](?) {
        val destination: ActorPath = path
        def createMessage(id: Long): Any = f(id)
        def behavior: Actor.Receive = behavior
      }
    }
    def deliver(f: Long => Any) = new taskBehavior(f)
    def !!(f: Long => Any) = new taskBehavior(f)
  }
  */
}
