package io.dac.merkleindex
import io.dac.merkleindex.validating.ValidatingImpl
import cats.instances.all._

import scala.util.Success

/**
  * Created by dcollins on 12/20/16.
  */
class ValidatingSpec extends AbstractSpec {
  "An Index" should "return the values inserted" in {
    ValidatingImpl.execute { index =>
      for {
        _ <- index.insert("hello", "10")
        v <- index.lookup("hello")
      } yield v
    } shouldEqual Success("10")
  }

  it should "return values inserted with many values" in {
    ValidatingImpl.execute { index =>
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
    } shouldEqual Success("40")
  }

}
