package io.dac.merkleindex.validating

import cats.free.Free
import cats.Eq
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.eq._
import io.dac.merkleindex.memory.MerkleImpl
import io.dac.merkleindex.trivial.TrivialImpl

import scala.util.{Failure, Success, Try}

/**
  * Created by dcollins on 12/20/16.
  */
object ValidatingImpl {
  import io.dac.merkleindex.dsl.StringIndexDsl._

  def execute[A: Eq](program: Dsl[Command] => Free[Command, A]): Try[A] = {
    for {
      trivial <- TrivialImpl.execute(program)
      memory <- MerkleImpl.execute(program)
    } yield {
      if (memory === trivial) {
        Success(memory)
      } else {
        Failure(new Exception(s"$trivial != $memory"))
      }
    }
  }
}


