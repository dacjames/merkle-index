package io.dac.tsindex

/**
  * Created by dcollins on 12/26/16.
  */
package object util {
  def nonNegative(x: Int): Option[Int] =
    if (x >= 0) Some(x)
    else None
}
