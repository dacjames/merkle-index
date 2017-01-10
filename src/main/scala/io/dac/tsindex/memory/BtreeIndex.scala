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
    * @param key Key to addInner
    * @param value Value to addInner
    * @return A new index with the key, value pair inserted
    */
  def add(key: Key, value: Value): BtreeIndex[Key, Value] =
    new BtreeIndex[Key, Value] {
      override def capacity = _capacity
      override def root = addInner(_root, key, value).asInstanceOf[BtreeNode]
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
      logger.debug(s"WTF: ${capacity}")

      if (items.length < capacity) {
        OuterNode(keys.toVector, values.toVector)
      } else {
        val initialKeys = keys.take(capacity - 1)
        val initialValues = values.take(capacity - 1)
        val initial: BtreeNode = OuterNode(initialKeys.toVector, initialValues.toVector)
        sorted.drop(capacity - 1).foldLeft[BtreeNode](initial) { (n, kv) => addInner(n, kv._1, kv._2) }
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
    private def deleteInner(key: Key): (DeleteResult, BtreeNode) = ???

    //    private def deleteInner(key: Key): (DeleteResult, BtreeNode) = {
//      logger.debug(s"Delete ${key} from \n${this.showTree}")
//      val (newResult, newNode) = this match {
//        case node@PointerNode(_, keys, children) =>
//          val (pos, child) = findChild(key, keys, children)
//          val (result, newChild) = child.deleteInner(key)
//
//          result match {
//            case DeleteResult.Noop =>
//              node match {
//                case node: InnerNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
//                case node: MiddleNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
//              }
//
//            case DeleteResult.Merge =>
//              if (keys.length < 2) {
//                // TODO: HACK to get around broken delete.
//                // THe problem case is when deleting 'd' from:
//                // △ [c] @0  ✓
//                //   □ [bg -> b70, bh -> b80, bi -> b90, bj -> b100] @1
//                //   □ [c -> 30, d -> 40] @1
//                // When d is removed, it triggers merge between both outer nodes
//                // eventually resulting the the middle node having only one child
//                // That breaks other logic currently.
//                // Not sure if the "no keys" case should be temporarily legal
//                // or this results from only merging, not balancing
//                // or overly aggressive split logic (*most likely*)
//                node match {
//                  case node: InnerNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
//                  case node: MiddleNode => (DeleteResult.Noop, node.vcopy(children = children.updated(pos, newChild)))
//                }
//              } else {
//                val newNode = node match {
//                  case node: MiddleNode =>
//                    if (pos < keys.length) {
//                      // the "normal" case, when not deleting from the last child
//                      val mergedChild = merge(newChild, children(pos + 1))
//
//                      node.vcopy(
//                        keys = keys.take(pos) ++ keys.drop(pos + 1),
//                        children = (children.take(pos) :+ mergedChild) ++ children.drop(pos + 2)
//                      )
//                    } else {
//                      // when deleting from the last child
//                      val mergedChild = merge(children(pos - 1), newChild)
//                      node.vcopy(
//                        keys = keys.take(pos - 1),
//                        children = children.take(pos - 1) :+ mergedChild
//                      )
//                    }
//                  case node: InnerNode =>
//
//                    if (pos < keys.length) {
//                      // the "normal" case, when not deleting from the last child
//                      val mergedChild = merge(newChild, children(pos + 1))
//
//                      val newChildren = if (mergedChild.isInstanceOf[MiddleNode2]) {
//                        fixSiblings(children, pos, mergedChild) ++
//                          children.slice(pos + 1, children.size + 1)
//                      } else {
//                        (children.take(pos) :+ mergedChild) ++ children.drop(pos + 2)
//                      }
//
//
//                      node.vcopy(
//                        keys = keys.take(pos) ++ keys.drop(pos + 1),
//                        children = newChildren
//                      )
//                    } else {
//                      // when deleting from the last child
//                      val mergedChild = merge(children(pos - 1), newChild)
//                      val newChildren = if (mergedChild.isInstanceOf[MiddleNode2]) {
//                        fixSiblings(children, pos, mergedChild)
//                      } else {
//                        children.take(pos - 1) :+ mergedChild
//                      }
//                      node.vcopy(
//                        keys = keys.take(pos - 1),
//                        children = newChildren
//                      )
//                    }
//
//
//                }
//                if (newNode.keys.size < (capacity / 2)) {
//                  (DeleteResult.Merge, newNode)
//                } else {
//                  (DeleteResult.Noop, newNode)
//                }
//              }
//          }
//
//        case node@OuterNode(_, keys, values) =>
//          nonNegative(keys.indexWhere(k => k == key)).map { pos =>
//            val newNode = node.vcopy(
//              keys = keys.take(pos) ++ keys.drop(pos + 1),
//              values = values.take(pos) ++ values.drop(pos + 1)
//            )
//            if (newNode.keys.size < (capacity / 2)) {
//              (DeleteResult.Merge, newNode)
//            } else {
//              (DeleteResult.Noop, newNode)
//            }
//          }.getOrElse {
//            (DeleteResult.Noop, node)
//          }
//      }
//      logger.debug(s"Delete ==> ${newResult}\n${newNode.showTree}")
//      (newResult, newNode)
//    }
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

  case class InnerNode2(version: Int,
                        keys: Vector[Key],
                        children: Vector[BtreeNode])
    extends PointerNode2[BtreeNode, InnerNode2, InnerNode2] {

    override def vcopy(version: Int, keys: Vector[Key], children: Vector[BtreeNode]) =
      copy(version=version, keys=keys, children=children)


    override def split: InnerNode2 = withPartition {
      case (mk, (lk, lc), (rk, rc)) =>

      val rightNode = InnerNode2(0, rk, rc)
      val leftNode = InnerNode2(0, lk, lc)

      InnerNode2(0, Vector(mk), Vector(leftNode, rightNode))
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


  private[this] def addInner(node: BtreeNode, key: Key, value: Value): BtreeNode = {
    logger.debug(s"Add2 ${key} -> ${value} to\n${node.showTree}")
    val newNode: BtreeNode = node match {

      case node @ InnerNode2(_, keys, children) =>
        val (pos, target) = node.findTarget(key)
        logger.debug(s"${showNode(node)} !=> ${showNode(target)}")
        val newNode = addInner(target, key, value) match {
          case it: MiddleNode2 =>
            val newChildren = fixSiblings(node.children.asInstanceOf[Vector[MiddleNode2]], pos, it)
            node.copy(version = node.version + 1, children = newChildren).checkInvarients()
          case it: InnerNode2 =>
            if (it.version == 0) {
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
            } else {
              node.copy(version = node.version + 1, children = children.updated(pos, it)).checkInvarients()
            }
          case newNode: Any =>
            throw new IllegalStateException(s"addInner(InnerNode2) should only ever return MiddleNode2 or InnerNode2, not ${newNode}")
        }
        if (newNode.isFull) {
          newNode.split
        } else {
          newNode
        }

      case node @ MiddleNode2(_, keys, children, sibling) =>
        val (pos, target) = node.findTarget(key)
        val newNode = addInner(target, key, value) match {
          case newNode: OuterNode =>
            node.copy(version = node.version + 1, children = children.updated(pos, newNode)).checkInvarients()
          case newNode: MiddleNode2 =>

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
            throw new IllegalStateException(s"addInner(OuterNode) should only ever return OuterNode or MiddleNode, not ${newNode}")
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


      case InnerNode2(version, keys, children) =>
        "⊙ " ++ keys.mkString("[", ", ", "]") ++ s" @${version}"

      case MiddleNode2(version, keys, children, sibling) =>
        val siblingString = if (sibling.isDefined) s" ✓" else " ✗"
        "△ " ++ keys.mkString("[", ", ", "]") ++ s" @${version} ${siblingString}"
    }
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


  private[this] def fixSiblings(children: Vector[MiddleNode2], pos: Int, newSibling: MiddleNode2): Vector[MiddleNode2] = {
    val fixedSibling = newSibling.copy(version = newSibling.version + 1, sibling = children.lift(pos + 1))
    children.slice(0, pos).foldRight(Vector(fixedSibling)) { (n, ch) =>
      n.copy(version = n.version + 1, sibling = Some(ch(0))) +: ch
    } ++ children.slice(pos + 1, children.size + 1)
  }



  private[this] def iterator(root: BtreeNode): Iterator[(Key, Value)] = {
    @tailrec
    def findMiddleOrOuter(node: BtreeNode): BtreeNode =
      node match {
        case InnerNode2(_, _, children) =>
          findMiddleOrOuter(children(0))
        case node: MiddleNode2 =>
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

      case middleRoot: MiddleNode2 =>
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

