package io.dac.merkleindex

import io.dac.merkleindex.trivial.TrivialImpl

/**
  * Created by dcollins on 12/17/16.
  */
class TrivialSpec extends AbstractSpec {

  "An Index" should "return the values inserted" in {
    TrivialImpl.execute { index =>
      for {
        _ <- index.insert("hello", "10")
        v <- index.lookup("hello")
      } yield v
    } shouldEqual "10"
  }

  it should "support deletes" in {

    an[NoSuchElementException] should be thrownBy {
      TrivialImpl.execute { index =>
        for {
          _ <- index.insert("hello", "10")
          _ <- index.delete("hello")
          v <- index.lookup("hello")
        } yield v
      }
    }
  }
}
