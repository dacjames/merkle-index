package io.dac.merkleindex

import cats.free.Free
import cats.{Id, ~>}

/**
  * Created by dcollins on 12/17/16.
  */
object MerkleImpl {

  import MerkleApi._

  class Impl(var state: MerkleIndex[String, String])
    extends (Command ~> Id) {

    override def apply[A](cmd: Command[A]): A = cmd match {
      case Add(key, value) => {
        state = state.add(key, value)
        value
      }
      case Get(key) => {
        state(key)
      }
    }
  }

  def interpreter: Command ~> Id =
    new Impl(MerkleIndex.empty)

  def execute[A](program: Free[Command, A]): Id[A] =
    program.foldMap(interpreter)
}
