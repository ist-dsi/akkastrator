package pt.ulisboa.tecnico.dsi.akkastrator

import scala.concurrent.duration.{FiniteDuration, DurationInt}

object Backoff {
  /**
   * Exponential back off
   *  0 =                               100 millis
   *  1 =                               200 millis
   *  2 =                               400 millis
   *  3 =                               800 millis
   *  4 =                     1 seconds 600 millis
   *  5 =                     3 seconds 200 millis
   *  6 =                     6 seconds 400 millis
   *  7 =                    12 seconds 800 millis
   *  8 =                    25 seconds 600 millis
   *  9 =                    51 seconds 200 millis
   * 10 =          1 minutes 42 seconds 400 millis
   * 11 =          3 minutes 24 seconds 800 millis
   * 12 =          6 minutes 49 seconds 600 millis
   * 13 =         13 minutes 39 seconds 200 millis
   * 14 =         27 minutes 18 seconds 400 millis
   * 15 =         54 minutes 36 seconds 800 millis
   * 16 = 1 hours 49 minutes 13 seconds 600 millis
   * 17 = 3 hours 38 minutes 27 seconds 200 millis
   * 18 = 7 hours 16 minutes 54 seconds 400 millis
   */
  def exponential(iteration: Int): FiniteDuration = scala.math.pow(2, iteration).round * 100.milliseconds
  def exponential(startIteration: Int = 0, maxIteration: Int = Int.MaxValue)(iteration: Int): FiniteDuration = {
    exponential(Math.min(maxIteration, startIteration + iteration))
  }
  //This exponential backoff is capped the 18th iteration or ≃ 7h16m
  def exponentialCapped(iteration: Int): FiniteDuration = exponential(0, maxIteration = 18)(iteration)

}
