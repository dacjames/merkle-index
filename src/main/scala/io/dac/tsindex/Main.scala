package io.dac.tsindex

import io.dac.tsindex.inmemory.BtreeIndex


/**
  * Created by dcollins on 12/3/16.
  */
object Main extends App {

  val x = BtreeIndex[Int, String](1 -> "a", 2 -> "b", 3 -> "c")
  val y = x.add(3, "a")
  val x1 = BtreeIndex[Int, String](1 -> "a", 2 -> "b").add(3, "c")
  val z = BtreeIndex[Int, String](1 -> "a")


  println(x.showTree)
  println(y.showTree)
  println(x1.showTree)
  assert(x == x1)

//  println(BtreeIndex[Int, String](2).add(3).add(5).add(7).showTree)
//  println(BtreeIndex[Int, String](2).add(3).add(5).add(7).add(7).showTree)
//  println(BtreeIndex[Int, String](2, 3, 13, 14, 5, 7).showTree)
//  println(BtreeIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21).showTree)
//  println(BtreeIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8).showTree)
//  println(BtreeIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8, 22, 23).showTree)
//  println(BtreeIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8, 22, 23, 1, 0, 9, 10).showTree)

  println(BtreeIndex[Int, String](
    22 -> "a",
    5 -> "b",
    32 -> "c",
    8 -> "d",
    12 -> "e",
    17 -> "f",
    25 -> "g",
    35 -> "h",
    41 -> "i",
    43 -> "j",
    50 -> "k",
    61 -> "l",
    64 -> "m").showTree)


}
