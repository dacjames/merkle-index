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
        _ <- index.delete("d")
        v <- index.lookup("h")
      } yield v
    } shouldEqual "80"

    an[NoSuchElementException] should be thrownBy {
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
          _ <- index.delete("d")
          v <- index.lookup("d")
        } yield v
      }
    }

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
        _ <- index.delete("i")
        v <- index.lookup("h")
      } yield v
    } shouldEqual "80"
  }

  "An InnerNode" should "have a vcopy method that mirrors the behavior of copy with updated versioning" in {
    val index = BtreeIndex.empty[String, String]
    val inner = index.Inner(Vector.empty, Vector.empty)

    inner.vcopy() shouldEqual inner
    inner.vcopy(keys = Vector("asdf")) shouldEqual index.Inner(1, Vector("asdf"), Vector.empty)
    inner.vcopy(children = Vector(inner)) shouldEqual index.Inner(0, Vector.empty, Vector(inner))
    inner.vcopy(keys = Vector("asdf"), children = Vector(inner)) shouldEqual index.Inner(1, Vector("asdf"), Vector(inner))

  }

  "An OuterNode" should "have a vcopy method that mirrors the behavior of copy with updated versioning" in {
    val index = BtreeIndex.empty[String, String]
    val outer = index.Outer(Vector.empty, Vector.empty, None)

    outer.vcopy() shouldEqual outer
    outer.vcopy(keys = Vector("asdf")) shouldEqual index.Outer(1, Vector("asdf"), Vector.empty, None)
    outer.vcopy(values = Vector("qwerty")) shouldEqual index.Outer(0, Vector.empty, Vector("qwerty"), None)
    outer.vcopy(keys = Vector("asdf"), values = Vector("qwerty")) shouldEqual index.Outer(1, Vector("asdf"), Vector("qwerty"), None)

  }

  "A BtreeIndex" should "support iteration over key, value pairs" in {
    val index = BtreeIndex(
      "a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5,
      "f" -> 6, "g" -> 7, "h" -> 8, "i" -> 9, "j" -> 10)


    index.map(_._1) shouldEqual List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    index.map(_._2) shouldEqual List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  }

}
