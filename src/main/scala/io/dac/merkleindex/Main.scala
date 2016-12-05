package io.dac.merkleindex




/**
  * Created by dcollins on 12/3/16.
  */
object Main extends App {

  val x = MerkleIndex[Int, String](1 -> "a", 2 -> "b", 3 -> "c")
  val y = x.add(3, "a")
  val x1 = MerkleIndex[Int, String](1 -> "a", 2 -> "b").add(3, "c")
  val z = MerkleIndex[Int, String](1 -> "a")


  println(x.showTree)
  println(y.showTree)
  println(x1.showTree)
  assert(x == x1)

//  println(MerkleIndex[Int, String](2).add(3).add(5).add(7).showTree)
//  println(MerkleIndex[Int, String](2).add(3).add(5).add(7).add(7).showTree)
//  println(MerkleIndex[Int, String](2, 3, 13, 14, 5, 7).showTree)
//  println(MerkleIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21).showTree)
//  println(MerkleIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8).showTree)
//  println(MerkleIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8, 22, 23).showTree)
//  println(MerkleIndex[Int, String](2, 3, 13, 14, 5, 7, 16, 19, 20, 15, 21, 8, 22, 23, 1, 0, 9, 10).showTree)

//  println(MerkleIndex[Int, String](22,5,32,8,12,17,25,35,41,43,50,61,64).showTree)

}
