package io.dac.tsindex
import io.dac.tsindex.memory.{BtreeIndex, InMemoryImpl}

/**
  * Created by dcollins on 12/17/16.
  */
class BtreeSpec extends AbstractSpec {

  "An Index" should "return the values inserted" in {
    InMemoryImpl.execute { index =>
      for {
        _ <- index.insert("hello", "10")
        v <- index.lookup("hello")
      } yield v
    } shouldEqual "10"
  }

  it should "return values inserted with many values" in {
    InMemoryImpl.execute { index =>
      for {
        _ <- index.insert("a", "10")
        _ <- index.insert("b", "20")
        _ <- index.insert("c", "30")
        _ <- index.insert("d", "40")
        _ <- index.insert("e", "50")
        _ <- index.insert("f", "60")
        _ <- index.insert("g", "70")
        _ <- index.insert("h", "80")
        _ <- index.insert("i", "90")
        _ <- index.insert("j", "100")
        v <- index.lookup("d")
      } yield v
    } shouldEqual "40"
  }

  it should "support deletes" in {
    an[NoSuchElementException] should be thrownBy {
      InMemoryImpl.execute { index =>
        for {
          _ <- index.insert("hello", "10")
          _ <- index.delete("hello")
          v <- index.lookup("hello")
        } yield v
      }
    }
  }
  it should "support deletes with many values" in {

    println("DELETES =================== DELETES")

    InMemoryImpl.execute { index =>
      for {
        _ <- index.insert("a", "10")
        _ <- index.insert("b", "20")
        _ <- index.insert("c", "30")
        _ <- index.insert("d", "40")
        _ <- index.insert("e", "50")
        _ <- index.insert("f", "60")
        _ <- index.insert("g", "70")
        _ <- index.insert("h", "80")
        _ <- index.insert("i", "90")
        _ <- index.insert("j", "100")
        _ <- index.insert("aa", "a10")
        _ <- index.insert("ab", "a20")
        _ <- index.insert("ac", "a30")
        _ <- index.insert("ad", "a40")
        _ <- index.insert("ae", "a50")
        _ <- index.insert("af", "a60")
        _ <- index.insert("ag", "a70")
        _ <- index.insert("ah", "a80")
        _ <- index.insert("ai", "a90")
        _ <- index.insert("aj", "a100")
        _ <- index.insert("ba", "b10")
        _ <- index.insert("bb", "b20")
        _ <- index.insert("bc", "b30")
        _ <- index.insert("bd", "b40")
        _ <- index.insert("be", "b50")
        _ <- index.insert("bf", "b60")
        _ <- index.insert("bg", "b70")
        _ <- index.insert("bh", "b80")
        _ <- index.insert("bi", "b90")
        _ <- index.insert("bj", "b100")
        _ <- index.delete("d")
        v <- index.lookup("h")
      } yield v
    } shouldEqual "80"

//    an[NoSuchElementException] should be thrownBy {
//      InMemoryImpl.execute { index =>
//        for {
//          _ <- index.insert("a", "10")
//          _ <- index.insert("b", "20")
//          _ <- index.insert("c", "30")
//          _ <- index.insert("d", "40")
//          _ <- index.insert("e", "50")
//          _ <- index.insert("f", "60")
//          _ <- index.insert("g", "70")
//          _ <- index.insert("h", "80")
//          _ <- index.insert("i", "90")
//          _ <- index.insert("j", "100")
//          _ <- index.delete("d")
//          v <- index.lookup("d")
//        } yield v
//      }
//    }

//    InMemoryImpl.execute { index =>
//      for {
//        _ <- index.insert("a", "10")
//        _ <- index.insert("b", "20")
//        _ <- index.insert("c", "30")
//        _ <- index.insert("d", "40")
//        _ <- index.insert("e", "50")
//        _ <- index.insert("f", "60")
//        _ <- index.insert("g", "70")
//        _ <- index.insert("h", "80")
//        _ <- index.insert("i", "90")
//        _ <- index.insert("j", "100")
//        _ <- index.delete("i")
//        v <- index.lookup("h")
//      } yield v
//    } shouldEqual "80"
  }

  "An InnerNode" should "have a vcopy method that mirrors the behavior of copy with updated versioning" in {
    val index = BtreeIndex.empty[String, String]
    val inner = index.InnerNode(Vector.empty, Vector.empty)

    inner.vcopy() shouldEqual inner
    inner.vcopy(keys = Vector("asdf")) shouldEqual index.InnerNode(1, Vector("asdf"), Vector.empty)
    inner.vcopy(children = Vector(inner)) shouldEqual index.InnerNode(0, Vector.empty, Vector(inner))
    inner.vcopy(keys = Vector("asdf"), children = Vector(inner)) shouldEqual index.InnerNode(1, Vector("asdf"), Vector(inner))

  }

  "An OuterNode" should "have a vcopy method that mirrors the behavior of copy with updated versioning" in {
    val index = BtreeIndex.empty[String, String]
    val outer = index.OuterNode(Vector.empty, Vector.empty)

    outer.vcopy() shouldEqual outer
    outer.vcopy(keys = Vector("asdf")) shouldEqual index.OuterNode(1, Vector("asdf"), Vector.empty)
    outer.vcopy(values = Vector("qwerty")) shouldEqual index.OuterNode(0, Vector.empty, Vector("qwerty"))
    outer.vcopy(keys = Vector("asdf"), values = Vector("qwerty")) shouldEqual index.OuterNode(1, Vector("asdf"), Vector("qwerty"))

  }

  "A BtreeIndex" should "support iteration over key, value pairs" in {
    val index = BtreeIndex(
      "a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5,
      "f" -> 6, "g" -> 7, "h" -> 8, "i" -> 9, "j" -> 10)


    index.map(_._1) shouldEqual List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    index.map(_._2) shouldEqual List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  }

}
