package io.dac.tsindex.memory

import com.typesafe.scalalogging.LazyLogging
import io.dac.tsindex.util.nonNegative
import io.dac.tsindex.util.operators._

import scala.annotation.tailrec


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
      override def root = add2(_root, key, value).asInstanceOf[BtreeNode]
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
        OuterNode(keys.toVector, values.toVector)
      } else {
        val initialKeys = keys.take(capacity)
        val initialValues = values.take(capacity)
        val initial: BtreeNode = OuterNode(initialKeys.toVector, initialValues.toVector)
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
    def size: Int

    def isFull: Boolean = this.keys.size == capacity
    def nonFull: Boolean = !isFull
    def willBeFull: Boolean = this.keys.size == capacity - 1

    def showTree: String = showTreeInner(0, this)

    def get(key: Key): Option[Value] = {
      logger.debug(s"get $key from\n${this.showTree}")
      this match {
        case PointerNode(_, keys, children) =>
          val (_, child) = findChild(key, keys, children)
          child.get(key)

        case OuterNode(_, keys, values) =>
          nonNegative(keys.indexWhere(k => k == key)).map(values)

        case node: PointerNode2[_, _, _] =>
          val (pos, child) = findChild(key, node.keys, node.children)
          logger.debug(s"WTF: ${pos} ${showNode(node)}")
          child.get(key)

      }
    }

    def checkInvarients(): this.type = {
//      logger.debug(s"Checking\n${this.showTree}")
      this match {
        case OuterNode(_, keys, values) =>
          assert(keys.size == values.size)

        case node: PointerNode2[_, _, _] =>
          assert(node.keys.size + 1 == node.children.size)
          node.children.foreach(_.checkInvarients())

        case _ =>
          assert(false)

      }
      this
    }

    def apply(key: Key): Value = get(key).get

    def delete(key: Key): BtreeNode = {
      val (_, newTree) = deleteInner(key)
      newTree
    }

    private def deleteInner(key: Key): (DeleteResult, BtreeNode) = {
      logger.debug(s"Delete ${key} from \n${this.showTree}")
      val (newResult, newNode) = this match {
        case node@PointerNode(_, keys, children) =>
          val (pos, child) = findChild(key, keys, children)
          val (result, newChild) = child.deleteInner(key)

          result match {
            case DeleteResult.Noop =>
              node match {
                case node: InnerNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
                case node: MiddleNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
              }

            case DeleteResult.Merge =>
              if (keys.length < 2) {
                // TODO: HACK to get around broken delete.
                // THe problem case is when deleting 'd' from:
                // △ [c] @0  ✓
                //   □ [bg -> b70, bh -> b80, bi -> b90, bj -> b100] @1
                //   □ [c -> 30, d -> 40] @1
                // When d is removed, it triggers merge between both outer nodes
                // eventually resulting the the middle node having only one child
                // That breaks other logic currently.
                // Not sure if the "no keys" case should be temporarily legal
                // or this results from only merging, not balancing
                // or overly aggressive split logic (*most likely*)
                node match {
                  case node: InnerNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
                  case node: MiddleNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
                }
              } else {
                val newNode = node match {
                  case node: MiddleNode =>
                    if (pos < keys.length) {
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
                        keys = keys.take(pos - 1),
                        children = children.take(pos - 1) :+ mergedChild
                      )
                    }
                  case node: InnerNode =>

                    if (pos < keys.length) {
                      // the "normal" case, when not deleting from the last child
                      val mergedChild = merge(newChild, children(pos + 1))

                      val newChildren = if (mergedChild.isInstanceOf[MiddleNode]) {
                        fixSiblings(children, pos, mergedChild) ++
                          children.slice(pos + 1, children.size + 1)
                      } else {
                        (children.take(pos) :+ mergedChild) ++ children.drop(pos + 2)
                      }


                      node.vcopy(
                        keys = keys.take(pos) ++ keys.drop(pos + 1),
                        children = newChildren
                      )
                    } else {
                      // when deleting from the last child
                      val mergedChild = merge(children(pos - 1), newChild)
                      val newChildren = if (mergedChild.isInstanceOf[MiddleNode]) {
                        fixSiblings(children, pos, mergedChild)
                      } else {
                        children.take(pos - 1) :+ mergedChild
                      }
                      node.vcopy(
                        keys = keys.take(pos - 1),
                        children = newChildren
                      )
                    }


                }
                if (newNode.keys.size < (capacity / 2)) {
                  (DeleteResult.Merge, newNode)
                } else {
                  (DeleteResult.Noop, newNode)
                }
              }
          }

        case node@OuterNode(_, keys, values) =>
          nonNegative(keys.indexWhere(k => k == key)).map { pos =>
            val newNode = node.vcopy(
              keys = keys.take(pos) ++ keys.drop(pos + 1),
              values = values.take(pos) ++ values.drop(pos + 1)
            )
            if (newNode.keys.size < (capacity / 2)) {
              (DeleteResult.Merge, newNode)
            } else {
              (DeleteResult.Noop, newNode)
            }
          }.getOrElse {
            (DeleteResult.Noop, node)
          }
      }
      logger.debug(s"Delete ==> ${newResult}\n${newNode.showTree}")
      (newResult, newNode)
    }

    def add(key: Key, value: Value): BtreeNode = {
      logger.debug(s"Add ${key}->${value} to ${showNode(this)}")
      val newNode = this match {
        case node@OuterNode(_, keys, values) =>
          // TODO: convert to using .isFull
          if (keys.size < capacity) {
            val (newKeys, newValues) = insert(keys, values)(key, value)
            node.vcopy(keys = newKeys, values = newValues)
          } else {
            val (middleKey, left, right) = split(this, key, value)

            val newKeys: Vector[Key] =
              Vector(middleKey)
            val newChildren: Vector[BtreeNode] =
              Vector(left, right)

            MiddleNode(newKeys, newChildren)
          }

        case node@MiddleNode(_, keys, children, sibling) =>
          // This only handles the initial case, we should not reach here when we have a parent
          if (node.isFull) {
            val (middleKey, left, right) = split(node, key, value)
            val newKeys = Vector(middleKey)

            InnerNode(newKeys, Vector(left, right))
          } else {
            var pos = 0
            while (pos < keys.size && keys(pos) < key) pos += 1
            val target = children(pos)

            if (target.isFull) {
              val (middleKey, left, right) = split(target, key, value)
              val newKeys = insert(keys)(middleKey)
              val newChildren = children.slice(0, pos) ++
                Vector(left, right) ++
                children.slice(pos + 1, children.size + 1)

              node.vcopy(keys = newKeys, children = newChildren)
            } else {
              val newChild = target.add(key, value)
              val newChildren = children.updated(pos, newChild)
              node.vcopy(children = newChildren)
            }

          }

        case node@InnerNode(_, keys, children) =>
          if (this.isFull) {
            val (middleKey, left, right) = split(this, key, value)
            val newKeys = Vector(middleKey)

            InnerNode(newKeys, Vector(left, right))
          } else {
            var pos = 0
            while (pos < keys.size && keys(pos) < key) pos += 1
            val target = children(pos)

            if (target.isFull) {
              val (middleKey, left, right) = split(target, key, value)
              val newKeys = insert(keys)(middleKey)


              val newChildren =
                if (pos > 0 && children(pos - 1).isInstanceOf[MiddleNode]) {
                  // Fix sibling pointer in children when splitting middle nodes
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
                if (pos > 0 && children(pos - 1).isInstanceOf[OuterNode]) {
                  fixSiblings(children, pos, newChild)
                } else {
                  children.updated(pos, newChild)
                }

              node.vcopy(children = newChildren)
            }
          }
      }
      logger.debug(s"Added ${key}->${value} ==>\n${newNode.showTree}")
      newNode
    }
  }

  sealed abstract class PointerNode extends BtreeNode {
    def keys: Vector[Key]
    def children: Vector[BtreeNode]
  }


  object PointerNode {
    def unapply(node: PointerNode): Option[(Int, Vector[Key], Vector[BtreeNode])] =
      node match {
        case InnerNode(version, keys, children) => Some((version, keys, children))
        case MiddleNode(version, keys, children, _) => Some((version, keys, children))
      }

  }

  sealed abstract class PointerNode2[ChildNode <: BtreeNode, ParentNode <: BtreeNode,
                                     Node <: PointerNode2[ChildNode, ParentNode, Node]]
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

    def withPartition[A](f: (Key, (Vector[Key], Vector[ChildNode]), (Vector[Key], Vector[ChildNode])) => A) = {
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

  case class RootNode2[InnerOrRoot <: BtreeNode](version: Int,
                                                 keys: Vector[Key],
                                                 children: Vector[InnerOrRoot])
    extends PointerNode2[InnerOrRoot, RootNode2[InnerOrRoot], RootNode2[InnerOrRoot]] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[InnerOrRoot]) =
      copy(version=version, keys=keys, children=children)



    override def split: RootNode2[InnerOrRoot] = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

        val leftNode = RootNode2(0, lk, lc)
        val rightNode = RootNode2(0, rk, rc)

        RootNode2(0, Vector(mk), Vector(leftNode, rightNode)).asInstanceOf[RootNode2[InnerOrRoot]]
    }
  }

  case class InnerNode2(version: Int,
                        keys: Vector[Key],
                        children: Vector[MiddleNode2])
    extends PointerNode2[MiddleNode2, RootNode2[InnerNode2], InnerNode2] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[MiddleNode2]) =
      copy(version=version, keys=keys, children=children)


    override def split: RootNode2[InnerNode2] = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

      val rightNode = InnerNode2(0, rk, rc)
      val leftNode = InnerNode2(0, lk, lc)

      RootNode2(0, Vector(mk), Vector(leftNode, rightNode))
    }
  }

  case class MiddleNode2(version: Int,
                         keys: Vector[Key],
                         children: Vector[OuterNode],
                         sibling: Option[MiddleNode2])
    extends PointerNode2[OuterNode, InnerNode2, MiddleNode2] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[OuterNode]) =
      copy(version=version, keys=keys, children=children)

    override def split: InnerNode2 = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

        val rightNode = MiddleNode2(0, rk, rc, None)
        val leftNode = MiddleNode2(0, lk, lc, Some(rightNode))

        logger.debug(s"WTF: ${showNode(leftNode)}, ${showNode(rightNode)}")

        InnerNode2(0, Vector(mk), Vector(leftNode, rightNode))
    }
  }


  case class InnerNode(version: Int,
                       keys: Vector[Key],
                       children: Vector[BtreeNode])
    extends PointerNode {

    override def size: Int = children.size

    def vcopy(keys: Vector[Key] = null, children: Vector[BtreeNode] = null) = {
      (keys, children) match {
        case (null, null)     => this
        case (null, children) => this.copy(this.version, children = children)
        case (keys, null)     => this.copy(version = nextVersion(this.version), keys = keys)
        case (keys, children) => this.copy(version = nextVersion(this.version), keys = keys, children = children)
      }
    }
  }

  object InnerNode {
    def apply(keys: Vector[Key],
              children: Vector[BtreeNode]): InnerNode = {
      InnerNode(0, keys, children)
    }
  }

  case class MiddleNode(version: Int,
                         keys: Vector[Key],
                         children: Vector[BtreeNode],
                         sibling: Option[MiddleNode])
    extends PointerNode {

    override def size: Int = children.size

    def vcopy(keys: Vector[Key] = null, children: Vector[BtreeNode] = null, sibling: Option[MiddleNode] = null) = {
      (keys, children, sibling) match {
        case (null, null, null)       => this
        case (null, children, null)     => this.copy(this.version, children = children)
        case (keys, null, null)       => this.copy(version = nextVersion(this.version), keys = keys)
        case (keys, children, null)     => this.copy(version = nextVersion(this.version), keys = keys, children = children)
        case (null, null, sibling)    => this.copy(version = nextVersion(this.version), sibling = sibling)
        case (null, children, sibling)  => this.copy(version = nextVersion(this.version), children = children, sibling = sibling)
        case (keys, null, sibling)       => this.copy(version = nextVersion(this.version), keys = keys, sibling = sibling)
        case (keys, children, sibling)     => this.copy(version = nextVersion(this.version), keys = keys, children = children, sibling = sibling)
      }
    }
  }

  object MiddleNode {
    def apply(keys: Vector[Key], children: Vector[BtreeNode]): MiddleNode =
      MiddleNode(0, keys, children, None)
    def apply(keys: Vector[Key], children: Vector[BtreeNode], sibling: MiddleNode): MiddleNode =
      MiddleNode(0, keys, children, Some(sibling))
    def apply(keys: Vector[Key], children: Vector[BtreeNode], sibling: Option[MiddleNode]): MiddleNode =
      MiddleNode(0, keys, children, sibling)
  }




  case class OuterNode(version: Int,
                       keys: Vector[Key],
                       values: Vector[Value]) extends BtreeNode {

    override def size: Int = values.size

    def uncheckedInsert(key: Key, value: Value): OuterNode = {
      val (newKeys, newValues) = insertAbstract(keys, values)(key, value)
      this.vcopy(newKeys, newValues)
    }

    def split: MiddleNode2 = {
      val middlePos = this.keys.size / 2
      val middleKey = this.keys(middlePos)

      val leftKeys = this.keys.slice(0, middlePos)
      val rightKeys = this.keys.slice(middlePos, this.keys.size + 1)

      val leftValues = this.values.slice(0, middlePos)
      val rightValues = this.values.slice(middlePos, this.values.size + 1)

      val leftNode = OuterNode(leftKeys, leftValues)
      val rightNode = OuterNode(rightKeys, rightValues)

      MiddleNode2(0, Vector(middleKey), Vector(leftNode, rightNode), None)
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

  sealed trait DeleteResult
  object DeleteResult {
    case object Noop extends DeleteResult
    case object Merge extends DeleteResult
  }

  sealed trait AddResult
  object AddResult {
    case object Noop extends AddResult
    case object Fixup extends AddResult
    case object Split extends AddResult
  }

  private[this] def nextVersion(v: Int): Int =
    v + 1

  private[this] def add3(node: BtreeNode, key: Key, value: Value): BtreeNode = {
    logger.debug(s"Add2 ${key} -> ${value} to\n${node.showTree}")
    val newNode: BtreeNode = ???

    logger.debug(s"Added  ${key} -> ${value}  ==>\n${newNode.showTree}")
    newNode.checkInvarients()
  }

  private[this] def add2(node: BtreeNode, key: Key, value: Value): BtreeNode = {
    logger.debug(s"Add2 ${key} -> ${value} to\n${node.showTree}")
    val newNode: BtreeNode = node match {

      case node @ RootNode2(_, keys, children) =>
        val (pos, target) = node.findTarget(key)
        logger.debug(s"${showNode(node)} !=> ${showNode(target)}")
        val newNode = add2(target, key, value) match {
          case it: RootNode2[_] =>
            // BUG: This rootNode may or may not have been the result of a split.
            // We're not handling the case where it is properly
            node.copy(version = node.version + 1, children = children.updated(pos, it)).checkInvarients()
          case it: InnerNode2 =>
            node.copy(version = node.version + 1, children = children.updated(pos, it)).checkInvarients()

          case it: Any =>
            logger.debug(s"?=> ${showNode(it)}")
            throw new IllegalStateException(s"add2(Inner or Root) should only ever return RootNode2 or InnerNode2, not ${it}")
        }
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node @ InnerNode2(_, keys, children) =>
        val (pos, target) = node.findTarget(key)
        logger.debug(s"${showNode(node)} !=> ${showNode(target)}")
        val newNode = add2(target, key, value) match {
          case it: MiddleNode2 =>
            node.copy(version = node.version + 1, children = children.updated(pos, it)).checkInvarients()
          case it: InnerNode2 =>
            val newChildren = children.slice(0, pos) ++ it.children ++ children.slice(pos + 1, children.size + 1)
            val newKeys =
              keys.slice(0, pos) ++
                it.keys ++
                keys.slice(pos, keys.size + 1)
            node.copy(
              version = node.version + 1,
              keys = newKeys,
              children = newChildren
            ).checkInvarients()
          case newNode: Any =>
            throw new IllegalStateException(s"add2(OuterNode) should only ever return MiddleNode2 or InnerNode2, not ${newNode}")
        }
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node @ MiddleNode2(_, keys, children, sibling) =>
        val (pos, target) = node.findTarget(key)
        val newNode = add2(target, key, value) match {
          case newNode: OuterNode =>
            node.copy(version = node.version + 1, children = children.updated(pos, newNode)).checkInvarients()
          case newNode: MiddleNode2 =>
            // not fixing sibling pointers
            val newChildren =
              children.slice(0, pos) ++
              newNode.children ++
              children.slice(pos + 1, children.size + 1)
            val newKeys =
              keys.slice(0, pos) ++
              newNode.keys ++
              keys.slice(pos, keys.size + 1)

            node.copy(
              version = node.version + 1,
              keys = newKeys,
              children = newChildren
            ).checkInvarients()
          case newNode: Any =>
            throw new IllegalStateException(s"add2(OuterNode) should only ever return OuterNode or MiddleNode, not ${newNode}")
        }

        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node @ OuterNode(_, keys, values) =>
        val (newKeys, newValues) = insert(keys, values)(key, value)
        val newNode = node.vcopy(keys = newKeys, values = newValues)
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

    }


    logger.debug(s"Added  ${key} -> ${value}  ==>\n${newNode.showTree}")
    newNode.checkInvarients()
    newNode
  }


  private[this] def findChild[Node <: BtreeNode](key: Key, keys: Vector[Key], children: Vector[Node]): (Int, Node) = {
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


  private[this] def insertAbstract[A](keys: Vector[Key], values: Vector[A])(key: Key, value: A): (Vector[Key], Vector[A]) = {
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

  private[this] def showTreeInner(level: Int, node: BtreeNode): String = {
    val indent = (0 until level).map(_ => "  ").mkString
    node match {
      case node: OuterNode =>
        indent ++ showNode(node)

      case node @ PointerNode(_, _, children) =>
        indent ++ showNode(node) ++ "\n" ++
          children.map(c => showTreeInner(level + 1, c)).mkString("\n")

      case node: PointerNode2[_, _, _] =>
        indent ++ showNode(node) ++ "\n" ++
          node.children.map(c => showTreeInner(level + 1, c)).mkString("\n")

      case _ => {
        throw new IllegalStateException(s"WTF: Tree is ${node.isInstanceOf[InnerNode]} ${node.isInstanceOf[MiddleNode]} ${node.isInstanceOf[OuterNode]}")
      }
    }
  }

  private[this] def showNode(node: BtreeNode): String = {
    node match {
      case OuterNode(version, keys, values) =>
        val itemString = keys.zip(values).map(kv => s"${kv._1} -> ${kv._2}")
        "□ " ++ itemString.mkString("[", ", ", "]") ++ s" @${version}"


      case MiddleNode(version, keys, children, sibling) =>
        val siblingString = if (sibling.isDefined) s" ✓" else " ✗"
        "△ " ++ keys.mkString("[", ", ", "]") ++ s" @${version} ${siblingString}"

      case InnerNode(version, keys, children) =>
        "⊙ " ++ keys.mkString("[", ", ", "]") ++ s" @${version}"


      case RootNode2(version, keys, children) =>
        "○ " ++ keys.mkString("[", ", ", "]") ++ s" @${version}"

      case InnerNode2(version, keys, children) =>
        "⊙ " ++ keys.mkString("[", ", ", "]") ++ s" @${version}"

      case MiddleNode2(version, keys, children, sibling) =>
        val siblingString = if (sibling.isDefined) s" ✓" else " ✗"
        "△ " ++ keys.mkString("[", ", ", "]") ++ s" @${version} ${siblingString}"
    }
  }


  private[this] def split(node: BtreeNode, key: Key, value: Value): (Key, BtreeNode, BtreeNode) = {
    logger.debug(s"Split ${showNode(node)} ${key}->${value}")
    val middlePos = node.size / 2
    val middleKey = node.keys(middlePos)

    val (leftNode, rightNode) = node match {
      case node @ OuterNode(_, keys, values) => {
        val middlePos = size / 2
        val middleKey = keys(middlePos)

        val leftKeys = keys.slice(0, middlePos)
        val rightKeys = keys.slice(middlePos, keys.size + 1)

        val leftValues = values.slice(0, middlePos)
        val rightValues = values.slice(middlePos, values.size + 1)

        if (key < middleKey) {
          val (newKeys, newValues) = insert(leftKeys, leftValues)(key, value)
          val rightNode = OuterNode(rightKeys, rightValues)
          val leftNode = OuterNode(newKeys, newValues)
          (leftNode, rightNode)
        } else {
          val (newKeys, newValues) = insert(rightKeys, rightValues)(key, value)
          val rightNode = OuterNode(newKeys, newValues)
          val leftNode = OuterNode(leftKeys, leftValues)
          (leftNode, rightNode)
        }
      }
      case node @ PointerNode(_, keys, children) =>
        val leftKeys = keys.slice(0, middlePos)
        val rightKeys = keys.slice(middlePos + 1, keys.size + 1)
        val leftChildren = children.slice(0, middlePos + 1)
        val rightChildren = children.slice(middlePos + 1, children.size + 1)

        val (left, right) = node match {
          case node: InnerNode =>
            (InnerNode(leftKeys, leftChildren), InnerNode(rightKeys, rightChildren))

          case node: MiddleNode =>
            val left = MiddleNode(leftKeys, leftChildren)
            val right = MiddleNode(rightKeys, rightChildren, left)
            (left, right)
        }
        logger.debug(s"wtf: ${showNode(left)} ${showNode(right)}")
        if (key < middleKey) {
          (left.add(key, value), right)
        } else {
          (left, right.add(key, value))
        }
    }

    logger.debug(s"==> ${showNode(leftNode)} «${middleKey}» ${showNode(rightNode)}")

    (middleKey, leftNode, rightNode)
  }

  private[this] def merge(left: BtreeNode, right: BtreeNode): BtreeNode = {
    logger.debug("Merging...")
    logger.debug("\n" + left.showTree)
    logger.debug("\n" + right.showTree)

    // left and right will always be the same type
    val newNode: BtreeNode = (left, right) match {
      case (InnerNode(_, leftkeys, leftchildren), InnerNode(_, rightkeys, rightchildren)) =>
        InnerNode(leftkeys ++ rightkeys, leftchildren ++ rightchildren)

      case (MiddleNode(_, leftkeys, leftchildren, leftsibling), MiddleNode(_, rightkeys, rightchildren, rightsibling)) =>
        MiddleNode(leftkeys ++ rightkeys, leftchildren ++ rightchildren, rightsibling)

      case (OuterNode(_, leftkeys, leftvalues), OuterNode(_, rightkeys, rightvalues)) =>
        OuterNode(leftkeys ++ rightkeys, leftvalues ++ rightvalues)

      case _ => throw new IllegalStateException(s"Tried to merge differing node types")
    }
    logger.debug(s"==>")
    logger.debug(s"\n" + newNode.showTree)
    newNode
  }


  private[this] def fixSiblings(children: Vector[BtreeNode], pos: Int, newSibling: BtreeNode) =
    children.slice(0, pos).foldRight(Vector(newSibling)) { (n, ch) =>
      n.asInstanceOf[MiddleNode].vcopy(sibling = Some(ch(0).asInstanceOf[MiddleNode])) +: ch
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
            val child = currentNode.children(currentChild).asInstanceOf[OuterNode]
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

    }
  }


}

object BtreeIndex {
  private val defaultCapacity = 4

  def empty[K: Ordering, V] = new BtreeIndex[K, V] {
    override def capacity = defaultCapacity
    override def root = OuterNode()
  }

  def withBranchingFactor[K: Ordering, V](n: Int)(items: (K, V)*) =
    new BtreeIndex[K, V] {
      override def capacity = n
      override def root = OuterNode()
    }.construct(items)

  def apply[K: Ordering, V](items: (K, V)*) =
    new BtreeIndex[K, V] {
      override def capacity = defaultCapacity
      override def root = OuterNode()
    }.construct(items)

}

