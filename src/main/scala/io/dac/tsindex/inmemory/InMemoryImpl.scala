package io.dac.tsindex.inmemory

import cats.free.Free
import cats.{Id, ~>}

/**
  * Created by dcollins on 12/17/16.
  */
object InMemoryImpl {

  import io.dac.tsindex.dsl.StringIndexDsl._

  private final class Impl(var state: BtreeIndex[String, String])
    extends (Command ~> Id) {

    override def apply[A](cmd: Command[A]): A = cmd match {
      case Insert(key, value) => {
        state = state.add(key, value)
        value
      }
      case Lookup(key) => {
        state(key)
      }
      case Delete(key) => {
        state = state.delete(key)
      }
    }
  }

  def interpreter: Command ~> Id =
    new Impl(BtreeIndex.empty)

  def execute[A](program: Dsl[Command] => Free[Command, A])(implicit api: Dsl[Command]): Id[A] =
    program(api).foldMap(interpreter)
}
