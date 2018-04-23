package pt.tecnico.dsi.akkastrator

import scala.annotation.tailrec

import shapeless.{::, HList, HNil}

object HListConstraints {
  trait TaskComapped[Dependencies <: HList] {
    type Results <: HList
    def results(l: Dependencies): Results
  }
  object TaskComapped {
    type Aux[DL <: HList, RL <: HList] = TaskComapped[DL] { type Results = RL }
    
    implicit def nil: Aux[HNil, HNil] = new TaskComapped[HNil]{
      type Results = HNil
      def results(l: HNil): HNil = HNil
    }
  
    implicit def cons[H, T <: HList, DL <: HList](implicit tc: TaskComapped[T]): Aux[FullTask[H, DL] :: T, H :: tc.Results] =
      new TaskComapped[FullTask[H, DL] :: T] {
        type Results = H :: tc.Results
        def results(l: FullTask[H, DL] :: T): Results = l.head.unsafeResult :: tc.results(l.tail)
      }
    
    /** This definition using existentials states that the dependencies of each FullTask are irrelevant.
      * Or more concretely the induction case is defined for:
      *  `FullTask[H, _] :: T`
      *    as opposed to
      *  `FullTask[H, DL] :: T` */
    implicit def consExistential[H, T <: HList](implicit tc: TaskComapped[T]): Aux[FullTask[H, _] :: T, H :: tc.Results] =
      new TaskComapped[FullTask[H, _] :: T] {
        type Results = H :: tc.Results
        def results(l: FullTask[H, _] :: T): Results = l.head.unsafeResult :: tc.results(l.tail)
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
