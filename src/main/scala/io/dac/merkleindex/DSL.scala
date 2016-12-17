package io.dac.merkleindex

import cats._
import cats.data._
import cats.free._

import scala.collection.mutable


/**
  * Created by dcollins on 12/11/16.
  */
sealed abstract class AbstractMerkleApi[ValueType] {

  sealed abstract class Command[+V]

  case class Add(key: String, value: ValueType)
    extends Command[ValueType]

  case class Get(key: String)
    extends Command[ValueType]


  class Dsl[F[_]](implicit I: Inject[Command, F]) {

    private[this] def inject[A](cmd: Command[A]): Free[F, A] =
      Free.inject[Command, F](cmd)

    def add(name: String, value: ValueType): Free[F, ValueType] =
      inject(Add(name, value))

    def get(name: String): Free[F, ValueType] =
      inject(Get(name))
  }
  object Dsl {
    implicit def dsl[F[_]](implicit I: Inject[Command, F]): Dsl[F] = new Dsl[F]
  }

}

object MerkleApi extends AbstractMerkleApi[String]



