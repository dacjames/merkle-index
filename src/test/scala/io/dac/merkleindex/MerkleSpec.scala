package io.dac.merkleindex
import io.dac.merkleindex.memory.MerkleImpl

/**
  * Created by dcollins on 12/17/16.
  */
class MerkleSpec extends AbstractSpec {

  "An Index" should "return the values inserted" in {
    MerkleImpl.execute { index =>
      for {
        _ <- index.insert("hello", "10")
        v <- index.lookup("hello")
      } yield v
    } shouldEqual "10"
  }

  it should "return values inserted with many values" in {
    MerkleImpl.execute { index =>
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
      MerkleImpl.execute { index =>
        for {
          _ <- index.insert("hello", "10")
          _ <- index.delete("hello")
          v <- index.lookup("hello")
        } yield v
      }
    }
  }
  it should "support deletes with many values" in {
    MerkleImpl.execute { index =>
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
      MerkleImpl.execute { index =>
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

    MerkleImpl.execute { index =>
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

}
