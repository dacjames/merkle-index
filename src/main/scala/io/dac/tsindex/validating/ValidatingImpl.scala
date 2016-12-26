package io.dac.tsindex.validating

import cats.free.Free
import cats.Eq
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.eq._
import io.dac.tsindex.memory.InMemoryImpl
import io.dac.tsindex.trivial.TrivialImpl

import scala.util.{Failure, Success, Try}

/**
  * Created by dcollins on 12/20/16.
  */
object ValidatingImpl {
  import io.dac.tsindex.dsl.StringIndexDsl._

  def execute[A: Eq](program: Dsl[Command] => Free[Command, A]): Try[A] = {
    for {
      trivial <- TrivialImpl.execute(program)
      memory <- InMemoryImpl.execute(program)
    } yield {
      if (memory === trivial) {
        Success(memory)
      } else {
        Failure(new Exception(s"$trivial != $memory"))
      }
    }
  }
}


