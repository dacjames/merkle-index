package io.dac.tsindex.memory

import io.dac.tsindex.util.StringHasher
import io.dac.tsindex.util.nonNegative

/**
  * Created by dcollins on 12/26/16.
  */
abstract class BtreeIndex[Key: Ordering, Value] extends StringHasher {
  type HashCode = String
  def capacity: Int
  def root: BtreeNode

  private def _capacity = capacity
  private def _root = root

  /**
    * Retrieve a value from the index for a given key
    * @throws NoSuchElementException When the index does not contain the key
    * @param key
    * @return       Value for the given key
    *
    */
  def apply(key: Key): Value = this.get(key).get

  /**
    * Optionally retrieve a value from the index for a given key
    * @param key
    * @return       Some(value) if the index contains the Key, None otherwise
    *
    */
  def get(key: Key): Option[Value] = root.get(key)

  /**
    * Add a new key, value pair to the index
    * @param key Key to add
    * @param value Value to add
    * @return A new index with the key, value pair inserted
    */
  def add(key: Key, value: Value): BtreeIndex[Key, Value] =
    new BtreeIndex[Key, Value] {
      override def capacity = _capacity
      override def root = _root.add(key, value).asInstanceOf[BtreeNode]
    }

  /**
    * Delete a key, value pair from the index.
    * Has no effect if the index does not contain the key
    * @param key
    * @return A new index without the key and it's respective value.
    */
  def delete(key: Key): BtreeIndex[Key, Value] = {
    val newRoot: BtreeNode = this.root.delete(key)
    if (newRoot == this.root) this
    else new BtreeIndex[Key, Value] {
      override def capacity = _capacity
      override def root = newRoot.asInstanceOf[BtreeNode]
    }
  }

  /**
    * Bulk construct a new tree from a sequence of key, value pairs
    * @param items
    * @return
    */
  def construct(items: Seq[(Key, Value)]): BtreeIndex[Key, Value] = {
    val newRoot: BtreeNode = {
      val sorted = items.sortBy(_._1)
      val keys = sorted.map(_._1)
      val values = sorted.map(_._2)

      if (items.length <= capacity) {
        Outer(keys.toVector, values.toVector)
      } else {
        val initialKeys = keys.take(capacity)
        val initialValues = values.take(capacity)
        val initial: BtreeNode = Outer(initialKeys.toVector, initialValues.toVector)
        sorted.drop(capacity).foldLeft[BtreeNode](initial) { (n, kv) => n.add(kv._1, kv._2) }
      }
    }
    new BtreeIndex[Key, Value] {
      override def capacity = _capacity
      override def root = newRoot.asInstanceOf[BtreeNode]
    }
  }

  /**
    * Pretty print the Btree.
    */
  def showTree: String = _root.showTree

  sealed abstract class BtreeNode {
    def hashcode: String
    def keys: Vector[Key]

    def isFull: Boolean = this.keys.size == capacity
    def nonFull: Boolean = !isFull

    private[this] def findChild(key: Key, children: Vector[BtreeNode]): (Int, BtreeNode) = {
      var pos = 0
      while (pos < keys.length && implicitly[Ordering[Key]].gteq(key, keys(pos))) pos += 1
      (pos, children(pos))
    }

    def showTree: String = showTreeInner(0, this)

    def get(key: Key): Option[Value] = this match {
      case Inner(_, keys, children) => {
        val (_, child) = findChild(key, children)
        child.get(key)
      }
      case Outer(_, keys, values) =>
        nonNegative(keys.indexWhere(k => k == key)).map(values)
    }

    def apply(key: Key): Value = get(key).get

    def add(key: Key, value: Value): BtreeNode = this match {
      case Outer(hashcode, keys, values) =>
        if (keys.size < capacity) {

          val (newKeys, newValues) = insert(keys, values)(key, value)

          Outer(incHash(hashcode, key), newKeys, newValues)
        } else {
          val (middleKey, left, right) = split(this, key, value)

          val newKeys: Vector[Key] =
            Vector(middleKey)
          val newChildren: Vector[BtreeNode] =
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

    def delete(key: Key): BtreeNode = {
      val (_, newTree) = deleteInner(key)
      newTree
    }

    private def deleteInner(key: Key): (DeleteResult, BtreeNode) = this match {
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

  case class Inner(hashcode: HashCode,
                   keys: Vector[Key],
                   children: Vector[BtreeNode]) extends BtreeNode

  object Inner {
    def apply(keys: Vector[Key],
              children: Vector[BtreeNode]): Inner = {
      val newCode = chainHash(children.map(_.hashcode))
      Inner(newCode, keys, children)
    }
  }

  case class Outer(hashcode: HashCode,
                   keys: Vector[Key],
                   values: Vector[Value]) extends BtreeNode

  object Outer {
    def apply(): Outer = {
      Outer("", Vector.empty[Key], Vector.empty[Value])
    }
    def apply(keys: Vector[Key], values: Vector[Value]): Outer = {
      val newCode = chainHash(keys)
      Outer(newCode, keys, values)
    }
  }

  sealed trait DeleteResult
  object DeleteResult {
    case object Noop extends DeleteResult
    case object Merge extends DeleteResult
  }

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

  private[this] def showTreeInner(level: Int, tree: BtreeNode): String = {
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
  private[this] def split(node: BtreeNode, key: Key, value: Value): (Key, BtreeNode, BtreeNode) = node match {
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

  private[this] def merge(left: BtreeNode, right: BtreeNode): BtreeNode = {
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

}

object BtreeIndex {
  private val defaultCapacity = 4

  def empty[K: Ordering, V] = new BtreeIndex[K, V] {
    override def capacity = defaultCapacity
    override def root = Outer()
  }

  def withBranchingFactor[K: Ordering, V](n: Int)(items: (K, V)*) =
    new BtreeIndex[K, V] {
      override def capacity = n
      override def root = Outer()
    }.construct(items)

  def apply[K: Ordering, V](items: (K, V)*) =
    new BtreeIndex[K, V] {
      override def capacity = defaultCapacity
      override def root = Outer()
    }.construct(items)

}

