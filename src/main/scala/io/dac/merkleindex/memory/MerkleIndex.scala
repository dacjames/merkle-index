package io.dac.merkleindex.memory

import java.security.MessageDigest

import scala.util.hashing.MurmurHash3

/**
  * Created by dcollins on 12/4/16.
  */
sealed abstract class MerkleIndex[Key: Ordering, Value] {
  import MerkleIndex._

  def hashcode: HashCode
  def keys: Vector[Key]
  def capacity: Int

  def isFull: Boolean = this.keys.size == capacity
  def nonFull: Boolean = !isFull

  def showTree: String = showTreeInner(0, this)

  private[this] def insert(keys: Vector[Key], values: Vector[Value])(key: Key, value: Value): (Vector[Key], Vector[Value]) = {
    var pos = 0
    while (pos < keys.size && implicitly[Ordering[Key]].gteq(key, keys(pos))) pos += 1

    val newKeys = (keys.slice(0, pos) :+ key) ++ keys.slice(pos, keys.size + 1)
    val newValues = (values.slice(0, pos) :+ value) ++ values.slice(pos, values.size + 1)

    (newKeys, newValues)
  }

  private[this] def insert(keys: Vector[Key])(key: Key): Vector[Key] = {
    var pos = 0
    while (pos < keys.size && implicitly[Ordering[Key]].gteq(key, keys(pos))) pos += 1

    val newKeys = (keys.slice(0, pos) :+ key) ++ keys.slice(pos, keys.size + 1)
    newKeys
  }

  private[this] def showTreeInner(level: Int, tree: MerkleIndex[Key, Value]): String = {
    val indent = (0 until level).map(_ => "  ").mkString
    tree match {
      case Outer(hc, keys, values) => {
        val itemString = keys.zip(values).map(kv => s"${kv._1} -> ${kv._2}")
        indent ++ itemString.mkString("[", ", ", "]") ++ s" «${hc}»"
      }
      case Inner(hc, keys, children) => {
        indent ++ keys.mkString("[", ", ", "]") ++ s" «${hc}»\n" ++
          children.map(c => showTreeInner(level + 1, c)).mkString("\n")
      }
    }
  }

  private[this] def split(node: MerkleIndex[Key, Value], key: Key, value: Value): (Key, MerkleIndex[Key, Value], MerkleIndex[Key, Value]) = node match {
    case Outer(_, keys, values) => {
      val middlePos = keys.size / 2
      val middleKey = keys(middlePos)

      val leftKeys = keys.slice(0, middlePos)
      val rightKeys = keys.slice(middlePos, keys.size + 1)

      val leftValues = values.slice(0, middlePos)
      val rightValues = values.slice(middlePos, values.size + 1)

      if (implicitly[Ordering[Key]].lt(key, middleKey)) {
        val (newKeys, newValues) = insert(leftKeys, leftValues)(key, value)
        (middleKey, Outer(newKeys, newValues), Outer(rightKeys, rightValues))
      } else {
        val (newKeys, newValues) = insert(rightKeys, rightValues)(key, value)
        (middleKey, Outer(leftKeys, leftValues), Outer(newKeys, newValues))
      }
    }
    case Inner(_, keys, children) => {
      val middlePos = keys.size / 2
      val middleKey = keys(middlePos)

      val leftKeys = keys.slice(0, middlePos)
      val rightKeys = keys.slice(middlePos + 1, keys.size + 1)

      val leftChildren = children.slice(0, middlePos + 1)
      val rightChildren = children.slice(middlePos + 1, children.size + 1)

      val left = Inner(leftKeys, leftChildren)
      val right = Inner(rightKeys, rightChildren)

      if (implicitly[Ordering[Key]].lt(key, middleKey)) {
        (middleKey, left.add(key, value), right)
      } else {
        (middleKey, left, right.add(key, value))
      }
    }
  }

  private[this] def nonNegative(x: Int): Option[Int] =
    if (x >= 0) Some(x)
    else None

  private[this] def findChild(key: Key, children: Vector[MerkleIndex[Key, Value]]): (Int, MerkleIndex[Key, Value]) = {
    var pos = 0
    while (pos < keys.length && implicitly[Ordering[Key]].gteq(key, keys(pos))) pos += 1
    (pos, children(pos))
  }

  def apply(key: Key): Value = get(key).get
  def get(key: Key): Option[Value] = this match {
    case Inner(_, keys, children) => {
      val (_, child) = findChild(key, children)
      child.get(key)
    }
    case Outer(_, keys, values) =>
      nonNegative(keys.indexWhere(k => k == key)).map(values)
  }
  def add(key: Key, value: Value): MerkleIndex[Key, Value] = this match {
    case Outer(hashcode, keys, values) =>
      if (keys.size < capacity) {

        val (newKeys, newValues) = insert(keys, values)(key, value)

        Outer(incHash(hashcode, key), newKeys, newValues)
      } else {
        val (middleKey, left, right) = split(this, key, value)

        val newKeys: Vector[Key] =
          Vector(middleKey)
        val newChildren: Vector[MerkleIndex[Key, Value]] =
          Vector(left, right)

        Inner(newKeys, newChildren)
      }

    case Inner(hashcode, keys, children) => {
      if (this.isFull) {
        val (middleKey, left, right) = split(this, key, value)
        Inner(Vector(middleKey), Vector(left, right))
      } else {
        var pos = 0
        while (pos < keys.size && implicitly[Ordering[Key]].lt(keys(pos), key)) pos += 1
        val target = children(pos)

        if (target.isFull) {
          val (middleKey, left, right) = split(target, key, value)
          val newKeys = insert(keys)(middleKey)

          val newChildren =
            children.slice(0, pos) ++
              Vector(left, right) ++
              children.slice(pos + 1, children.size + 1)
          Inner(newKeys, newChildren)
        }
        else {
          val newChild = target.add(key, value)
          Inner(keys, children.updated(pos, newChild))
        }
      }
    }
  }
  private def merge(left: MerkleIndex[Key, Value], right: MerkleIndex[Key, Value]): MerkleIndex[Key, Value] = {

    println(s"Merging...")
    println(left.showTree)
    println(right.showTree)

    // left and right will always be the same type
    val newNode = (left, right) match {
      case (Inner(_, leftkeys, leftchildren), Inner(_, rightkeys, rightchildren)) =>
        Inner(leftkeys ++ rightkeys, leftchildren ++ rightchildren)

      case (Outer(_, leftkeys, leftvalues), Outer(_, rightkeys, rightvalues)) =>
        Outer(leftkeys ++ rightkeys, leftvalues ++ rightvalues)

      case _ => throw new IllegalStateException(s"Tried to merge Inner and Outer Nodes.")
    }
    println(s"==>")
    println(s"${newNode.showTree}")
    newNode
  }

  def delete(key: Key): MerkleIndex[Key, Value] = {
    val (_, newTree) = deleteInner(key)
    newTree
  }

  private def deleteInner(key: Key): (DeleteResult, MerkleIndex[Key, Value]) = this match {
    case node @ Inner(_, keys, children) => {
      val (pos, child) = findChild(key, children)
      val (result, newChild) = child.deleteInner(key)

      result match {
        case DeleteResult.Noop => (DeleteResult.Noop, node.copy(children = children.updated(pos, newChild)))
        case DeleteResult.Merge => {
          val newNode = if (pos < keys.length) {
            // the "normal" case, when not deleting from the last child
            val mergedChild = merge(newChild, children(pos + 1))
            node.copy(
              keys = keys.take(pos) ++ keys.drop(pos + 1),
              children = (children.take(pos) :+ mergedChild) ++ children.drop(pos + 2)
            )
          } else {
            // when deleting from the last child
            val mergedChild = merge(children(pos - 1), newChild)
            node.copy(
              keys = keys.take(pos - 1) ,
              children = children.take(pos - 1) :+ mergedChild
            )
          }

          if (newNode.keys.size < (capacity / 2)) {
            (DeleteResult.Merge, newNode)
          } else {
            (DeleteResult.Noop, newNode)
          }
        }
      }
    }
    case node @ Outer(_, keys, values) => {
      nonNegative(keys.indexWhere(k => k == key)).map{ pos =>
        val newNode = node.copy(
          keys = keys.take(pos) ++ keys.drop(pos + 1),
          values = values.take(pos) ++ values.drop(pos + 1)
        )
        if (newNode.keys.size < (capacity / 2)) {
          (DeleteResult.Merge, newNode)
        } else {
          (DeleteResult.Noop, newNode)
        }
      }.getOrElse{ (DeleteResult.Noop, node) }
    }
  }
}

object MerkleIndex {
  type HashCode = String
  val _capacity = 4

  case class Inner[Key: Ordering, Value](hashcode: HashCode,
                                         keys: Vector[Key],
                                         children: Vector[MerkleIndex[Key, Value]])
    extends MerkleIndex[Key, Value] {
    override def capacity = _capacity
  }

  object Inner {
    def apply[Key: Ordering, Value](keys: Vector[Key],
                                    children: Vector[MerkleIndex[Key, Value]]): Inner[Key, Value] = {
      val newCode = chainHash(children.map(_.hashcode))
      Inner(newCode, keys, children)
    }
  }

  case class Outer[Key: Ordering, Value]
  (hashcode: HashCode, keys: Vector[Key], values: Vector[Value])
    extends MerkleIndex[Key, Value] {
    override def capacity = _capacity
  }

  object Outer {
    def apply[Key: Ordering, Value](keys: Vector[Key],
                                    values: Vector[Value]): Outer[Key, Value] = {
      val newCode = chainHash(keys)
      Outer(newCode, keys, values)
    }
  }

  sealed trait DeleteResult
  object DeleteResult {
    case object Noop extends DeleteResult
    case object Merge extends DeleteResult
  }


  private[this] object Hasher {

    private val md5instance =
      MessageDigest.getInstance("MD5")

    def md5(s: String): String =
      md5instance.digest(s.getBytes).map("%02x".format(_)).mkString

    def murmur(s: String): String =
      Integer.toHexString(MurmurHash3.stringHash(s))
  }

  def incHash[A](a: HashCode, b: A): HashCode =
    Hasher.md5(a ++ b.toString)

  def chainHash[A](nodes: Seq[A]): HashCode =
    chainHash(nodes, "")

  def chainHash[A](nodes: Seq[A], hashCode: HashCode): HashCode =
    nodes.foldLeft(hashCode)(incHash)

  def apply[Key: Ordering, Value](items: (Key, Value)*): MerkleIndex[Key, Value] = {
    val sorted = items.sortBy(_._1)
    val keys = sorted.map(_._1)
    val values = sorted.map(_._2)

    if (items.length <= _capacity) {
      Outer(keys.toVector, values.toVector)
    } else {
      val initialKeys = keys.take(_capacity)
      val initialValues = values.take(_capacity)
      val initial = Outer[Key, Value](initialKeys.toVector, initialValues.toVector)
      sorted.drop(_capacity).foldLeft[MerkleIndex[Key, Value]](initial){(n, kv) => n.add(kv._1, kv._2)}
    }
  }

  def empty[K: Ordering, V] = MerkleIndex[K, V]()

}
