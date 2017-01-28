package io.dac.tsindex.inmemory

import com.typesafe.scalalogging.LazyLogging
import io.dac.tsindex.util.nonNegative
import io.dac.tsindex.util.operators._

import scala.annotation.tailrec


/**
  * Created by dcollins on 12/26/16.
  */
abstract class BtreeIndex[Key: Ordering, Value] extends Iterable[(Key, Value)] with LazyLogging {
  type HashCode = String
  protected def degree: Int
  protected def root: BtreeNode
  protected def checkInvariants: Boolean

  private[this] def _degree = degree
  private[this] def _root = root
  private[this] def _checkInvariants = checkInvariants

  /**
    * Retrieve a value from the index for a given key
    * @throws NoSuchElementException When the index does not contain the key
    * @param key
    * @return       Value for the given key
    *
    */
  def apply(key: Key): Value = root.apply(key)

  /**
    * Optionally retrieve a value from the index for a given key
    * @param key
    * @return       Some(value) if the index contains the Key, None otherwise
    *
    */
  def get(key: Key): Option[Value] = root.get(key)

  /**
    * Add a new key, value pair to the index
    * @param key Key to addInner
    * @param value Value to addInner
    * @return A new index with the key, value pair inserted
    */
  def add(key: Key, value: Value): BtreeIndex[Key, Value] =
    new BtreeIndex[Key, Value] {
      override def degree = _degree
      override def root = _root.add(key, value).asInstanceOf[BtreeNode]
      override def checkInvariants = _checkInvariants
    }

  /**
    * Delete a key, value pair from the index.
    * Has no effect if the index does not contain the key
    * @param key
    * @return A new index without the key and it's respective value.
    */
  def delete(key: Key): BtreeIndex[Key, Value] =
    new BtreeIndex[Key, Value] {
      override def degree = _degree
      override def root = _root.delete(key).asInstanceOf[BtreeNode]
      override def checkInvariants = _checkInvariants
    }

  /**
    * Delete multiple keys from the index at once.
    * Keys that do not exist are ignored
    * @param keys Keys to delete
    * @return A new with all the given keys removed.
    */
  def deleteAll(keys: Key*) =
    new BtreeIndex[Key, Value] {
      override def degree = _degree
      override def root = keys.foldLeft(_root)((n, k) => n.delete(k)).asInstanceOf[BtreeNode]
      override def checkInvariants = _checkInvariants
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

      if (items.length < degree) {
        OuterNode(keys.toVector, values.toVector)
      } else {
        val initialKeys = keys.take(degree - 1)
        val initialValues = values.take(degree - 1)
        val initial: BtreeNode = OuterNode(initialKeys.toVector, initialValues.toVector)
        sorted.drop(degree - 1).foldLeft[BtreeNode](initial) { (n, kv) => addInner(n, kv._1, kv._2) }
      }
    }
    new BtreeIndex[Key, Value] {
      override def degree = _degree
      override def root = newRoot.asInstanceOf[BtreeNode]
      override def checkInvariants = _checkInvariants
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
    def size: Int

    def isEmpty: Boolean = this.keys.isEmpty
    def isFull: Boolean = this.keys.size == degree
    def halfFull: Boolean = this.keys.size >= (degree / 2)
    def underHalfFull: Boolean = this.keys.size < (degree / 2)
    def overHalfFull: Boolean = this.keys.size > (degree / 2)

    def showTree: String = showTreeInner(0, this)

    def get(key: Key): Option[Value] =
      getInner(this, key)

    def apply(key: Key): Value =
      get(key).get

    def add(key: Key, value: Value): BtreeNode =
      addInner(this, key, value)

    def delete(key: Key): BtreeNode =
      deleteInner(this, key)

    def checkInvariants(): this.type = {
      if (_checkInvariants) {
        checkInvariantsInner(this)
      } else {
        this
      }
    }
  }

  sealed abstract class PointerNode[ChildNode <: BtreeNode, ParentNode <: BtreeNode,
                                     Node <: PointerNode[ChildNode, ParentNode, Node]]
    extends BtreeNode {
    def keys: Vector[Key]
    def children: Vector[ChildNode]

    override def size: Int =
      this.children.size


    def findTarget(key: Key): (Int, ChildNode) = {
      var pos = 0
      while (pos < keys.size && keys(pos) < key) pos += 1
      val target = children(pos)
      (pos, target)
    }

    def withPartition[A](f: (Key, (Vector[Key], Vector[ChildNode]), (Vector[Key], Vector[ChildNode])) => A): A = {
      val middlePos = this.keys.size / 2
      val middleKey = this.keys(middlePos)

      val leftKeys = this.keys.slice(0, middlePos)
      val rightKeys = this.keys.slice(middlePos + 1, this.keys.size + 1)

      val leftChildren = this.children.slice(0, middlePos + 1)
      val rightChildren = this.children.slice(middlePos + 1, this.children.size + 1)

      f(middleKey, (leftKeys, leftChildren), (rightKeys, rightChildren))
    }

    def split: ParentNode
    def vcopy(version: Int, keys: Vector[Key], children: Vector[ChildNode]): Node

  }

  case class InnerNode(version: Int,
                       keys: Vector[Key],
                       children: Vector[BtreeNode])
    extends PointerNode[BtreeNode, InnerNode, InnerNode] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[BtreeNode]) =
      copy(version=version, keys=keys, children=children)

    def hasMiddleChildren: Boolean =
      this.children.exists(_.isInstanceOf[MiddleNode])

    override def split: InnerNode = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

      val rightNode = InnerNode(0, rk, rc)
      val leftNode = InnerNode(0, lk, lc)

      InnerNode(0, Vector(mk), Vector(leftNode, rightNode))
    }
  }

  case class MiddleNode(version: Int,
                        keys: Vector[Key],
                        children: Vector[OuterNode],
                        sibling: Option[MiddleNode])
    extends PointerNode[OuterNode, InnerNode, MiddleNode] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[OuterNode]) =
      copy(version=version, keys=keys, children=children)

    override def split: InnerNode = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

        val rightNode = MiddleNode(0, rk, rc, None)
        val leftNode = MiddleNode(0, lk, lc, Some(rightNode))

        InnerNode(0, Vector(mk), Vector(leftNode, rightNode))
    }
  }


  case class OuterNode(version: Int,
                       keys: Vector[Key],
                       values: Vector[Value]) extends BtreeNode {

    override def size: Int = values.size

    def uncheckedInsert(key: Key, value: Value): OuterNode = {
      var pos = 0
      while (pos < keys.size && key >= keys(pos)) pos += 1

      val newKeys = (keys.slice(0, pos) :+ key) ++ keys.slice(pos, keys.size + 1)
      val newValues = (values.slice(0, pos) :+ value) ++ values.slice(pos, values.size + 1)

      OuterNode(0, newKeys, newValues)
    }

    def split: MiddleNode = {
      val middlePos = this.keys.size / 2
      val middleKey = this.keys(middlePos)

      val leftKeys = this.keys.slice(0, middlePos)
      val rightKeys = this.keys.slice(middlePos, this.keys.size + 1)

      val leftValues = this.values.slice(0, middlePos)
      val rightValues = this.values.slice(middlePos, this.values.size + 1)

      val leftNode = OuterNode(leftKeys, leftValues)
      val rightNode = OuterNode(rightKeys, rightValues)

      MiddleNode(0, Vector(middleKey), Vector(leftNode, rightNode), None)
    }


    def vcopy(keys: Vector[Key] = null, values: Vector[Value] = null) = {
      (keys, values) match {
        case (null, null)       => this
        case (null, values)     => this.copy(this.version, values = values)
        case (keys, null)       => this.copy(version = nextVersion(this.version), keys = keys)
        case (keys, values)     => this.copy(version = nextVersion(this.version), keys = keys, values = values)
      }
    }
  }

  object OuterNode {
    def apply(): OuterNode = {
      OuterNode(0, Vector.empty[Key], Vector.empty[Value])
    }
    def apply(keys: Vector[Key], values: Vector[Value]): OuterNode = {
      OuterNode(0, keys, values)
    }
  }


  private[this] def nextVersion(v: Int): Int =
    v + 1

  private[this] def checkInvariantsInner[Node <: BtreeNode](node: Node) = {
    node match {
      case OuterNode(_, keys, values) =>
        assert(keys.size == values.size)

      case node: MiddleNode =>
        assert(node.keys.size + 1 == node.children.size)
        node.children.foreach(_.checkInvariants())

      case node: InnerNode =>
        assert(node.keys.size + 1 == node.children.size)
        node.children.foreach(_.checkInvariants())
        node.children.collect {
          case child: MiddleNode => child
        }.sliding(2, 1).foreach { siblings =>
          assert(siblings(0).sibling.get eq siblings(1))
        }

      case _ =>
        assert(false)

    }
    node
  }

  private[this] def getInner(node: BtreeNode, key: Key): Option[Value] =
    node match {
      case OuterNode(_, keys, values) =>
        nonNegative(keys.indexWhere(k => k == key)).map(values)

      case node: PointerNode[_, _, _] =>
        val (pos, child) = findChild(key, node.keys, node.children)
        child.get(key)

    }


  private[this] def addInner(node: BtreeNode, key: Key, value: Value): BtreeNode = {
    logger.debug(s"Add ${key} -> ${value} to\n${node.showTree}")
    val newNode: BtreeNode = node match {

      case node @ InnerNode(_, keys, children) =>
        val (pos, target) = node.findTarget(key)

        val newNode = addInner(target, key, value) match {
          case newNode: MiddleNode =>
            val newChildren = fixSiblings(node.children.asInstanceOf[Vector[MiddleNode]], pos, newNode)
            node.copy(version = nextVersion(node.version), children = newChildren).checkInvariants()

          case newNode: InnerNode =>
            if (newNode.version == 0) {
              val newChildren = if (newNode.hasMiddleChildren) {
                fixSiblings(children.asInstanceOf[Vector[MiddleNode]], pos, pos, newNode.children.asInstanceOf[Vector[MiddleNode]])
              } else {
                children.slice(0, pos) ++ newNode.children ++ children.slice(pos + 1, children.size + 1)
              }
              val newKeys =
                keys.slice(0, pos) ++
                  newNode.keys ++
                  keys.slice(pos, keys.size + 1)
              node.copy(
                version = nextVersion(node.version),
                keys = newKeys,
                children = newChildren
              ).checkInvariants()
            } else {
              node.copy(version = nextVersion(node.version), children = children.updated(pos, newNode)).checkInvariants()
            }

          case newNode: Any =>
            throw new IllegalStateException(s"addInner(InnerNode2) should only ever return MiddleNode2 or InnerNode2, not ${newNode}")
        }
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node @ MiddleNode(_, keys, children, sibling) =>
        val (pos, target) = node.findTarget(key)
        val newNode = addInner(target, key, value) match {
          case newNode: OuterNode =>
            node.copy(version = nextVersion(node.version), children = children.updated(pos, newNode)).checkInvariants()

          case newNode: MiddleNode =>
            val newChildren =
              children.slice(0, pos) ++
              newNode.children ++
              children.slice(pos + 1, children.size + 1)

            val newKeys =
              keys.slice(0, pos) ++
              newNode.keys ++
              keys.slice(pos, keys.size + 1)

            node.copy(
              version = nextVersion(node.version),
              keys = newKeys,
              children = newChildren
            ).checkInvariants()

          case newNode: Any =>
            throw new IllegalStateException(s"addInner(OuterNode) should only ever return OuterNode or MiddleNode, not ${newNode}")
        }

        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node: OuterNode =>
        val newNode = node.uncheckedInsert(key, value)
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

    }

    logger.debug(s"Added  ${key} -> ${value}  ==>\n${newNode.showTree}")
    newNode
  }


  private[this] def deleteInner(node: BtreeNode, key: Key): BtreeNode = {
    logger.debug(s"Delete ${key} from \n${node.showTree}")

    val newNode = node match {
      case node @ OuterNode(version, keys, values) =>
        nonNegative(keys.indexWhere(_ == key)).map { pos =>
          node.copy(
            version = nextVersion(version),
            keys = keys.take(pos) ++ keys.drop(pos + 1),
            values = values.take(pos) ++ values.drop(pos + 1)
          )
        }.getOrElse(node)

      case node @ MiddleNode(version, keys, children, sibling) =>
        val (pos, child) = findChild(key, keys, children)
        val keyPos =
          if (pos == 0) 0
          else pos - 1

        val newChild = deleteInner(child, key).asInstanceOf[OuterNode]

        val ((leftPos, left), (rightPos, right)) = findSiblings(pos, children, newChild)

        // TODO strealing only takes one value at a time.
        if (left.underHalfFull && right.overHalfFull) {
          // steal from right
          val stolenKey = right.keys.head
          val stolenValue = right.values.head

          val newLeft = left.copy(
            version = nextVersion(left.version),
            keys = left.keys :+ stolenKey,
            values = left.values :+ stolenValue
          )

          val newRight = right.copy(
            version = nextVersion(right.version),
            keys = right.keys.drop(1),
            values = right.values.drop(1)
          )

          node.copy(
            version = nextVersion(node.version),
            keys = keys.updated(keyPos, newRight.keys.head),
            children = children.updated(leftPos, newLeft).updated(rightPos, newRight)
          ).checkInvariants()
        } else if (right.underHalfFull && left.overHalfFull) {
          // steal from left
          val stolenKey = left.keys.last
          val stolenValue = left.values.last

          val newLeft = left.copy(
            version = nextVersion(left.version),
            keys = left.keys.dropRight(1),
            values = left.values.dropRight(1)
          )

          val newRight = right.copy(
            version = nextVersion(right.version),
            keys = stolenKey +: right.keys,
            values = stolenValue +: right.values
          )

          node.copy(
            version = nextVersion(node.version),
            keys = keys.updated(keyPos, stolenKey),
            children = children.updated(leftPos, newLeft).updated(rightPos, newRight)
          ).checkInvariants()

        } else if (left.isEmpty) {

          val transient = right.split
          node.copy(
            version = nextVersion(node.version),
            keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
            children = children.take(leftPos) ++ transient.children ++ children.drop(rightPos + 1)
          ).checkInvariants()

        } else if (right.isEmpty) {

          val transient = left.split
          node.copy(
            version = nextVersion(node.version),
            keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
            children = children.take(leftPos) ++ transient.children ++ children.drop(rightPos + 1)
          ).checkInvariants()

        } else {

          node.copy(
            version = nextVersion(version),
            children = children.updated(pos, newChild),
            sibling = sibling
          ).checkInvariants()

        }

      case node @ InnerNode(version, keys, children) =>
        val (pos, child) = findChild(key, keys, children)
        val keyPos =
          if (pos == 0) 0
          else pos - 1

        val newChild = deleteInner(child, key)

        newChild match {
          case newChild: MiddleNode =>
            val middleChildren = children.asInstanceOf[Vector[MiddleNode]]
            val ((leftPos, left), (rightPos, right)) = findSiblings(pos, middleChildren, newChild)

            if (left.isEmpty) {

              val transient = right.split
              node.copy(
                version = nextVersion(version),
                keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
                children = fixSiblings(middleChildren, leftPos, rightPos, transient.children.asInstanceOf[Vector[MiddleNode]])
              ).checkInvariants()

            } else if (right.isEmpty) {

              val transient = left.split
              node.copy(
                version = nextVersion(version),
                keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
                children = fixSiblings(middleChildren, leftPos, rightPos, transient.children.asInstanceOf[Vector[MiddleNode]])
              ).checkInvariants()

            } else {
              node.copy(
                version = nextVersion(version),
                children = fixSiblings(middleChildren, pos, newChild)
              ).checkInvariants()
            }

          case newChild: InnerNode =>
            val innerChildren = children.asInstanceOf[Vector[InnerNode]]
            val ((leftPos, left), (rightPos, right)) = findSiblings(pos, innerChildren, newChild)

            if (left.isEmpty) {

              val transient = right.split
              node.copy(
                version = nextVersion(version),
                keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
                children = children.take(leftPos) ++ transient.children ++ children.drop(rightPos + 1)
              ).checkInvariants()

            } else if (right.isEmpty) {

              val transient = left.split
              node.copy(
                version = nextVersion(version),
                keys = keys.take(keyPos) ++ transient.keys ++ keys.drop(keyPos + 1),
                children = children.take(leftPos) ++ transient.children ++ children.drop(rightPos + 1)
              ).checkInvariants()

            } else {
              node.copy(
                version = nextVersion(version),
                children = children.updated(pos, newChild)
              ).checkInvariants()
            }

          case _ => throw new IllegalThreadStateException(s"deleteInner(${child.getClass}) should never return a node of type ${newChild.getClass}")

        }

    }

    logger.debug(s"Delete2 ${key} ==>\n${newNode.showTree}")
    newNode
  }

  private[this] def findChild[Node <: BtreeNode](key: Key, keys: Vector[Key], children: Vector[Node]): (Int, Node) = {
    var pos = 0
    while (pos < keys.length && key >= keys(pos)) pos += 1
    (pos, children(pos))
  }

  private[this] def findSiblings[Node <: BtreeNode](pos: Int, children: Vector[Node], newChild: Node): ((Int, Node), (Int, Node)) = {
    if (pos == children.size - 1) {
      (pos - 1 -> children(pos - 1), pos -> newChild)
    } else {
      (pos -> newChild, pos + 1 -> children(pos + 1))
    }
  }

  private[this] def showTreeInner(level: Int, node: BtreeNode): String = {
    val indent = (0 until level).map(_ => "  ").mkString
    node match {
      case node: OuterNode =>
        indent ++ showNode(node)

      case node: PointerNode[_, _, _] =>
        indent ++ showNode(node) ++ "\n" ++
          node.children.map(c => showTreeInner(level + 1, c)).mkString("\n")

      case _ => {
        throw new IllegalStateException(s"Some crazy bug in showTreeInner: ${node}")
      }
    }
  }

  private[this] def showNode(node: BtreeNode): String =
    node match {
      case OuterNode(version, keys, values) =>
        val itemString = keys.zip(values).map(kv => s"${kv._1} -> ${kv._2}")
        "□ " ++ itemString.mkString("[", ", ", "]") ++ s" @${version}"

      case InnerNode(version, keys, children) =>
        "⊙ " ++ keys.mkString("[", ", ", "]") ++ s" @${version}"

      case MiddleNode(version, keys, children, sibling) =>
        val siblingString = if (sibling.isDefined) s" ✓" else " ✗"
        "△ " ++ keys.mkString("[", ", ", "]") ++ s" @${version} ${siblingString}"
    }

  private[this] def fixSiblings(children: Vector[MiddleNode], pos: Int, newSibling: MiddleNode): Vector[MiddleNode] = {
    val fixedSibling = newSibling.copy(version = nextVersion(newSibling.version), sibling = children.lift(pos + 1))

    children.slice(0, pos).foldRight(Vector(fixedSibling)) { (n, ch) =>
      n.copy(version = nextVersion(n.version), sibling = Some(ch(0))) +: ch
    } ++ children.slice(pos + 1, children.size + 1)
  }

  private[this] def fixSiblings(children: Vector[MiddleNode], leftPos: Int, rightPos: Int, newSiblings: Vector[MiddleNode]): Vector[MiddleNode] = {
    val lastSibling = newSiblings.last
    val fixedSibling = lastSibling.copy(version = nextVersion(lastSibling.version), sibling = children.lift(rightPos + 1))

    (children.slice(0, leftPos) ++ newSiblings.dropRight(1)).foldRight(Vector(fixedSibling)) { (n, ch) =>
      n.copy(version = nextVersion(n.version), sibling = Some(ch(0))) +: ch
    } ++ children.slice(rightPos + 1, children.size + 1)
  }


  private[this] def iterator(root: BtreeNode): Iterator[(Key, Value)] = {
    @tailrec
    def findMiddleOrOuter(node: BtreeNode): BtreeNode =
      node match {
        case InnerNode(_, _, children) =>
          findMiddleOrOuter(children(0))
        case node: MiddleNode =>
          node
        case node: OuterNode =>
          node
      }

    val iterRoot = findMiddleOrOuter(root)
    iterRoot match {
      case outerRoot: OuterNode =>
        new Iterator[(Key, Value)] {
          private var currentNode = outerRoot
          private var currentPos = 0

          override def hasNext: Boolean =
            currentPos < currentNode.values.size

          override def next: (Key, Value) = {
            val key = currentNode.keys(currentPos)
            val value = currentNode.values(currentPos)
            currentPos += 1
            (key, value)
          }
        }

      case middleRoot: MiddleNode =>
        new Iterator[(Key, Value)] {
          private var currentNode = middleRoot
          private var currentChild = 0
          private var currentPos = 0

          def hasNext: Boolean =
            currentChild < currentNode.children.size && currentPos < currentNode.children(currentChild).size ||
              currentNode.sibling.isDefined

          override def next: (Key, Value) = {
            val child = currentNode.children(currentChild)
            val key = child.keys(currentPos)
            val value = child.values(currentPos)

            if (currentChild == currentNode.keys.length && currentPos == child.keys.length - 1 && currentNode.sibling.isDefined) {
              currentNode = currentNode.sibling.get
              currentChild = 0
              currentPos = 0
            } else if (currentPos == child.keys.length - 1) {
              currentChild += 1
              currentPos = 0
            } else {
              currentPos += 1
            }
            (key, value)
          }
        }

      case _ => throw new IllegalStateException("Impossible because we just searched to find either an OuterNode or MiddleNode")

    }
  }


}

object BtreeIndex {
  private val defaultDegree = 4
  private val defaultCheckInvariants = false

  def empty[K: Ordering, V] =
    withConfig()(List.empty[(K, V)]: _*)

  def apply[K: Ordering, V](items: (K, V)*): BtreeIndex[K, V] =
    withConfig()(items: _*)

  def withConfig[K: Ordering, V](degree: Int = defaultDegree, checkInvariants: Boolean = defaultCheckInvariants)(items: (K, V)*) = {
    val _degree = degree
    val _checkInvariants = checkInvariants

    new BtreeIndex[K, V] {
      override def degree = _degree
      override def root = OuterNode()
      override def checkInvariants = _checkInvariants
    }.construct(items)
  }


}

