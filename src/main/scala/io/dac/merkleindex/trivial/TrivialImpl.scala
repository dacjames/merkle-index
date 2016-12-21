package io.dac.merkleindex.trivial

import cats.free.Free
import cats.{Id, ~>}

import scala.collection.mutable

/**
  * Created by dcollins on 12/17/16.
  * A trivial implementation based on Scala's builtin TreeMap
  */
object TrivialImpl {
  import io.dac.merkleindex.dsl.StringIndexDsl._

  private final class Impl(var state: mutable.SortedMap[String, String])
    extends (Command ~> Id) {

    override def apply[A](cmd: Command[A]): Id[A] = cmd match {
      case Insert(key, value) => {
        state = state + (key -> value)
        value
      }
      case Lookup(key) => {
        state(key)
      }
      case Delete(key) => {
        state = state - key
      }
    }
  }

  def interpreter: Command ~> Id =
    new Impl(mutable.TreeMap.empty)

  def execute[A](program: Dsl[Command] => Free[Command, A])(implicit api: Dsl[Command]): Id[A] =
    program(api).foldMap(interpreter)

}
