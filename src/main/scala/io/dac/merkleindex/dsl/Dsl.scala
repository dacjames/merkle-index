package io.dac.merkleindex.dsl

import cats.free._


/**
  * Created by dcollins on 12/11/16.
  */
sealed abstract class AbstractIndexDsl[ValueType] {

  sealed abstract class Command[+V]

  case class Insert(key: String, value: ValueType)
    extends Command[ValueType]

  case class Lookup(key: String)
    extends Command[ValueType]

  case class Delete(key: String)
    extends Command[Unit]


  class Dsl[F[_]](implicit I: Inject[Command, F]) {

    private[this] def inject[A](cmd: Command[A]): Free[F, A] =
      Free.inject[Command, F](cmd)

    def insert(name: String, value: ValueType): Free[F, ValueType] =
      inject(Insert(name, value))

    def lookup(name: String): Free[F, ValueType] =
      inject(Lookup(name))

    def delete(name: String): Free[F, Unit] =
      inject(Delete(name))
  }
  object Dsl {
    implicit def dsl[F[_]](implicit I: Inject[Command, F]): Dsl[F] = new Dsl[F]
  }

}

object StringIndexDsl extends AbstractIndexDsl[String]



