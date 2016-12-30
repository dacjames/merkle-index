package io.dac.tsindex

/**
  * Created by dcollins on 12/26/16.
  */
package object util {
  def nonNegative(x: Int): Option[Int] =
    if (x >= 0) Some(x)
    else None



  object operators {
    private def _xor(a: Boolean, b: Boolean): Boolean =
      a != b

    implicit class BooleanOps(a: Boolean) {
      def xor(b: Boolean): Boolean = _xor(a, b)
      def ^^(b: Boolean): Boolean = _xor(a, b)
    }
  }
}
