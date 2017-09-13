package pt.tecnico.dsi.akkastrator

import scala.annotation.tailrec

import shapeless.{::, HList, HNil}

object HListConstraints {
  trait TaskComapped[DependenciesList <: HList] {
    type ResultsList <: HList
    def buildResultsList(l: DependenciesList): ResultsList
  }
  object TaskComapped {
    type Aux[DL <: HList, RL <: HList] = TaskComapped[DL] { type ResultsList = RL }
    
    // Summoner, see https://github.com/underscoreio/shapeless-guide for more info
    def apply[T <: HList](implicit tc: TaskComapped[T]): Aux[T, tc.ResultsList] = tc
    
    implicit def nil: Aux[HNil, HNil] = new TaskComapped[HNil]{
      type ResultsList = HNil
      def buildResultsList(l: HNil): HNil = HNil
    }
  
    implicit def cons[H, T <: HList, DL <: HList](implicit tc: TaskComapped[T]): Aux[FullTask[H, DL] :: T, H :: tc.ResultsList] =
      new TaskComapped[FullTask[H, DL] :: T] {
        type ResultsList = H :: tc.ResultsList
        def buildResultsList(l: FullTask[H, DL] :: T): ResultsList = l.head.unsafeResult :: tc.buildResultsList(l.tail)
      }
    
    /** This definition using existentials is needed for some methods of the DSL.
      * It states that the dependencies of each FullTask in the DependenciesList are irrelevant.
      * Or more concretely the induction case is defined for:
      *  `FullTask[H, _] :: T`
      *    as opposed to
      *  `FullTask[H, DL] :: T` */
    implicit def consExistential[H, T <: HList](implicit tc: TaskComapped[T]): Aux[FullTask[H, _] :: T, H :: tc.ResultsList] =
      new TaskComapped[FullTask[H, _] :: T] {
        type ResultsList = H :: tc.ResultsList
        def buildResultsList(l: FullTask[H, _] :: T): ResultsList = l.head.unsafeResult :: tc.buildResultsList(l.tail)
      }
  }
  
  implicit class taskHListOps[L <: HList](l: L)(implicit ev: TaskComapped[L]) {
    def foreach(f: FullTask[_, _] => Unit): Unit = {
      // The implicit TaskComaped ensures that every element of the HList L is of type FullTask[_, _]
      // So we annotated l with @unchecked because otherwise the compiler would think the match is not exhaustive, which it is.
      @tailrec def loop(l: HList): Unit = (l: @unchecked) match {
        case HNil => ()
        case (head: FullTask[_, _]) :: tail =>
          f(head)
          loop(tail)
      }
      loop(l)
    }
  }
}
