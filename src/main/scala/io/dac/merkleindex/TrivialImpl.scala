package io.dac.merkleindex

import cats.free.Free
import cats.{Id, ~>}

import scala.collection.mutable

/**
  * Created by dcollins on 12/17/16.
  */
object TrivialImpl {
  import MerkleApi._

  class Impl(state: mutable.TreeMap[String, String])
    extends (Command ~> Id) {

    override def apply[A](cmd: Command[A]): A = cmd match {
      case Add(key, value) => {
        state += (key -> value)
        value
      }
      case Get(key) => {
        state(key)
      }
    }
  }

  def interpreter: Command ~> Id =
    new Impl(mutable.TreeMap.empty)

  def execute[A](program: Free[Command, A]): Id[A] =
    program.foldMap(interpreter)

}
