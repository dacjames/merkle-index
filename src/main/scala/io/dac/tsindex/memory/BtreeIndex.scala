package io.dac.tsindex.memory

import com.typesafe.scalalogging.LazyLogging
import io.dac.tsindex.util.StringHasher
import io.dac.tsindex.util.nonNegative

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by dcollins on 12/26/16.
  */
abstract class BtreeIndex[Key: Ordering, Value] extends Iterable[(Key, Value)] with LazyLogging {
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
        Outer(keys.toVector, values.toVector, None)
      } else {
        val initialKeys = keys.take(capacity)
        val initialValues = values.take(capacity)
        val initial: BtreeNode = Outer(initialKeys.toVector, initialValues.toVector, None)
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

  override def iterator: Iterator[(Key, Value)] = iterator(_root)

  /**
    * Convenience conversion so that keys support basic boolean operators
    * @param key
    */
  private implicit class KeyOps(key: Key) {
    def lt(other: Key) = implicitly[Ordering[Key]].lt(key, other)
    def < = lt _
    def gt(other: Key) = implicitly[Ordering[Key]].gt(key, other)
    def > = gt _
    def lteq(other: Key) = implicitly[Ordering[Key]].lteq(key, other)
    def <= = lteq _
    def gteq(other: Key) = implicitly[Ordering[Key]].gteq(key, other)
    def >= = gteq _
  }

  sealed abstract class BtreeNode {
    def version: Int
    def keys: Vector[Key]

    def isFull: Boolean = this.keys.size == capacity
    def nonFull: Boolean = !isFull

    def showTree: String = showTreeInner(0, this)

    def get(key: Key): Option[Value] = this match {
      case Inner(_, keys, children) => {
        val (_, child) = findChild(key, keys, children)
        child.get(key)
      }
      case Outer(_, keys, values, _) =>
        nonNegative(keys.indexWhere(k => k == key)).map(values)
    }

    def apply(key: Key): Value = get(key).get

    def delete(key: Key): BtreeNode = {
      val (_, newTree) = deleteInner(key)
      newTree
    }

    private def deleteInner(key: Key): (DeleteResult, BtreeNode) = this match {
      case node @ Inner(_, keys, children) => {
        val (pos, child) = findChild(key, keys, children)
        val (result, newChild) = child.deleteInner(key)

        result match {
          case DeleteResult.Noop => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
          case DeleteResult.Merge => {
            val newNode = if (pos < keys.length) {
              // the "normal" case, when not deleting from the last child
              val mergedChild = merge(newChild, children(pos + 1))
              node.vcopy(
                keys = keys.take(pos) ++ keys.drop(pos + 1),
                children = (children.take(pos) :+ mergedChild) ++ children.drop(pos + 2)
              )
            } else {
              // when deleting from the last child
              val mergedChild = merge(children(pos - 1), newChild)
              node.vcopy(
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
      case node @ Outer(_, keys, values, sibling) => {
        nonNegative(keys.indexWhere(k => k == key)).map{ pos =>
          val newNode = node.vcopy(
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

    def add(key: Key, value: Value): BtreeNode = this match {
      case node@Outer(_, keys, values, sibling) =>
        if (keys.size < capacity) {

          val (newKeys, newValues) = insert(keys, values)(key, value)

          node.vcopy(keys = newKeys, values = newValues)
        } else {
          val (middleKey, left, right) = split(this, key, value)

          val newKeys: Vector[Key] =
            Vector(middleKey)
          val newChildren: Vector[BtreeNode] =
            Vector(left, right)

          Inner(newKeys, newChildren)
        }

      case node@Inner(_, keys, children) => {
        if (this.isFull) {
          val (middleKey, left, right) = split(this, key, value)
          node.vcopy(keys = Vector(middleKey), children = Vector(left, right))
        } else {
          var pos = 0
          while (pos < keys.size && keys(pos) < key) pos += 1
          val target = children(pos)

          if (target.isFull) {
            val (middleKey, left, right) = split(target, key, value)
            val newKeys = insert(keys)(middleKey)


            val newChildren =
              if (pos > 0 && children(pos - 1).isInstanceOf[Outer]) {
                // Fix sibling pointer in children when splitting outer nodes
                // left.asInstanceOf is safe because of the children.isInstanceOf check
                fixSiblings(children, pos, left) ++
                  Vector(right) ++
                  children.slice(pos + 1, children.size + 1)

              } else {
                children.slice(0, pos) ++
                  Vector(left, right) ++
                  children.slice(pos + 1, children.size + 1)
              }


            node.vcopy(keys = newKeys, children = newChildren)
          }
          else {
            val newChild = target.add(key, value)


            val newChildren =
              if (pos > 0 && children(pos - 1).isInstanceOf[Outer]) {
                fixSiblings(children, pos, newChild)
              } else {
                children.updated(pos, newChild)
              }

            node.vcopy(children = newChildren)
          }
        }
      }
    }

  }

  case class Inner(version: Int,
                   keys: Vector[Key],
                   children: Vector[BtreeNode]) extends BtreeNode {
    def vcopy(keys: Vector[Key] = null, children: Vector[BtreeNode] = null) = {
      (keys, children) match {
        case (null, null)     => this
        case (null, children) => this.copy(this.version, children = children)
        case (keys, null)     => this.copy(version = nextVersion(this.version), keys = keys)
        case (keys, children) => this.copy(version = nextVersion(this.version), keys = keys, children = children)
      }
    }
  }

  object Inner {
    def apply(keys: Vector[Key],
              children: Vector[BtreeNode]): Inner = {
      Inner(0, keys, children)
    }
  }

  case class Outer(version: Int,
                   keys: Vector[Key],
                   values: Vector[Value],
                   sibling: Option[Outer]) extends BtreeNode {
    def vcopy(keys: Vector[Key] = null, values: Vector[Value] = null, sibling: Option[Outer] = null) = {
      (keys, values, sibling) match {
        case (null, null, null)       => this
        case (null, values, null)     => this.copy(this.version, values = values)
        case (keys, null, null)       => this.copy(version = nextVersion(this.version), keys = keys)
        case (keys, values, null)     => this.copy(version = nextVersion(this.version), keys = keys, values = values)
        case (null, null, sibling)    => this.copy(version = nextVersion(this.version), sibling = sibling)
        case (null, values, sibling)  => this.copy(version = nextVersion(this.version), values = values, sibling = sibling)
        case (keys, null, sibling)       => this.copy(version = nextVersion(this.version), keys = keys, sibling = sibling)
        case (keys, values, sibling)     => this.copy(version = nextVersion(this.version), keys = keys, values = values, sibling = sibling)
      }
    }
  }

  object Outer {
    def apply(): Outer = {
      Outer(0, Vector.empty[Key], Vector.empty[Value], None)
    }
    def apply(keys: Vector[Key], values: Vector[Value], sibling: Option[Outer]): Outer = {
      Outer(0, keys, values, sibling)
    }
  }

  sealed trait DeleteResult
  object DeleteResult {
    case object Noop extends DeleteResult
    case object Merge extends DeleteResult
  }

  private[this] def nextVersion(v: Int): Int =
    v + 1

  private[this] def findChild(key: Key, keys: Vector[Key], children: Vector[BtreeNode]): (Int, BtreeNode) = {
    var pos = 0
    while (pos < keys.length && key >= keys(pos)) pos += 1
    (pos, children(pos))
  }

  private[this] def insert(keys: Vector[Key], values: Vector[Value])(key: Key, value: Value): (Vector[Key], Vector[Value]) = {
    var pos = 0
    while (pos < keys.size && key >= keys(pos)) pos += 1

    val newKeys = (keys.slice(0, pos) :+ key) ++ keys.slice(pos, keys.size + 1)
    val newValues = (values.slice(0, pos) :+ value) ++ values.slice(pos, values.size + 1)

    (newKeys, newValues)
  }

  private[this] def insert(keys: Vector[Key])(key: Key): Vector[Key] = {
    var pos = 0
    while (pos < keys.size && key >= keys(pos)) pos += 1

    val newKeys = (keys.slice(0, pos) :+ key) ++ keys.slice(pos, keys.size + 1)
    newKeys
  }

  private[this] def showTreeInner(level: Int, tree: BtreeNode): String = {
    val indent = (0 until level).map(_ => "  ").mkString
    tree match {
      case Outer(version, keys, values, sibling) => {
        val itemString = keys.zip(values).map(kv => s"${kv._1} -> ${kv._2}")
        val siblingString = if (sibling.isDefined) s"✓" else "✗"
        indent ++ itemString.mkString("[", ", ", "]") ++ s" «${version}» ${siblingString}"
      }
      case Inner(version, keys, children) => {
        indent ++ keys.mkString("[", ", ", "]") ++ s" «${version}»\n" ++
          children.map(c => showTreeInner(level + 1, c)).mkString("\n")
      }
    }
  }
  private[this] def split(node: BtreeNode, key: Key, value: Value): (Key, BtreeNode, BtreeNode) = node match {
    case node @ Outer(_, keys, values, sibling) => {
      val middlePos = keys.size / 2
      val middleKey = keys(middlePos)

      val leftKeys = keys.slice(0, middlePos)
      val rightKeys = keys.slice(middlePos, keys.size + 1)

      val leftValues = values.slice(0, middlePos)
      val rightValues = values.slice(middlePos, values.size + 1)

      if (key < middleKey) {
        val (newKeys, newValues) = insert(leftKeys, leftValues)(key, value)
        val rightNode = Outer(rightKeys, rightValues, node.sibling)
        val leftNode = Outer(newKeys, newValues, Some(rightNode))
        (middleKey, leftNode, rightNode)
      } else {
        val (newKeys, newValues) = insert(rightKeys, rightValues)(key, value)
        val rightNode = Outer(newKeys, newValues, node.sibling)
        val leftNode = Outer(leftKeys, leftValues, Some(rightNode))
        (middleKey, leftNode, rightNode)
      }
    }
    case node @ Inner(_, keys, children) => {
      val middlePos = keys.size / 2
      val middleKey = keys(middlePos)

      val leftKeys = keys.slice(0, middlePos)
      val rightKeys = keys.slice(middlePos + 1, keys.size + 1)

      val leftChildren = children.slice(0, middlePos + 1)
      val rightChildren = children.slice(middlePos + 1, children.size + 1)

      val left = Inner(leftKeys, leftChildren)
      val right = Inner(rightKeys, rightChildren)

      if (key < middleKey) {
        (middleKey, left.add(key, value), right)
      } else {
        (middleKey, left, right.add(key, value))
      }
    }
  }

  private[this] def merge(left: BtreeNode, right: BtreeNode): BtreeNode = {
    logger.debug("Merging...")
    logger.debug(left.showTree)
    logger.debug(right.showTree)

    // left and right will always be the same type
    val newNode = (left, right) match {
      case (Inner(_, leftkeys, leftchildren), Inner(_, rightkeys, rightchildren)) =>
        Inner(leftkeys ++ rightkeys, leftchildren ++ rightchildren)

      case (Outer(_, leftkeys, leftvalues, leftsibling), Outer(_, rightkeys, rightvalues, rightsibling)) =>
        Outer(leftkeys ++ rightkeys, leftvalues ++ rightvalues, rightsibling)

      case _ => throw new IllegalStateException(s"Tried to merge Inner and Outer Nodes.")
    }
    logger.debug(s"==>")
    logger.debug(s"${newNode.showTree}")
    newNode
  }

  private[this] def fixSiblings(children: Vector[BtreeNode], pos: Int, newSibling: BtreeNode) =
    children.slice(0, pos).foldRight(Vector(newSibling)) { (n, ch) =>
      n.asInstanceOf[Outer].vcopy(sibling = Some(ch(0).asInstanceOf[Outer])) +: ch
    }



//  private[this] def recordVersions(node: BtreeNode): Map[Int, Int] = {
//    var versionInfo = Map.empty[Int, Int]
//    unsafeForeachNode(node) { n =>
//      versionInfo += (System.identityHashCode(n) -> n.version)
//    }
//    versionInfo
//  }

  private[this] def iterator(root: BtreeNode): Iterator[(Key, Value)] = {
    @tailrec
    def findOuter(node: BtreeNode): Outer =
      node match {
        case Inner(_, _, children) =>
          findOuter(children(0))
        case node: Outer =>
          node
      }

    new Iterator[(Key, Value)] {
      private var currentNode = findOuter(root)
      private var currentPos = 0

      override def hasNext: Boolean =
        currentPos < currentNode.values.size ||
          currentNode.sibling.isDefined

      override def next: (Key, Value) = {
        val key = currentNode.keys(currentPos)
        val value = currentNode.values(currentPos)
        if (currentPos == currentNode.keys.length - 1 && currentNode.sibling.isDefined) {
          currentNode = currentNode.sibling.get
          currentPos = 0
        } else {
          currentPos += 1
        }
        (key, value)
      }
    }
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

