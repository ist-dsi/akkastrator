package pt.tecnico.dsi.akkastrator

/** A function that calculates how many votes are needed to achieve a quorum, given the number of destinations. */
trait MinimumVotes extends (Int => Int) {
  def apply(numberOfDestinations: Int): Int
}

/** A MinimumVotes function where a majority (at least 50%) of votes are needed to achieve a quorum. */
object Majority extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = 1 + numberOfDestinations / 2
}

/**
  * A MinimumVotes function where at least `n` votes are needed to achieve a quorum.
  * If `n` is bigger than the number of destinations this function will behave like the `All` function.
  */
case class AtLeast(n: Int) extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = Math.min(n, numberOfDestinations)
}

/** A MinimumVotes function where all the votes are needed to achieve a quorum. */
object All extends MinimumVotes {
  def apply(numberOfDestinations: Int): Int = numberOfDestinations
}