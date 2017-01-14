package io.dac.tsindex

import io.dac.tsindex.model.{TsiMetric, TsiSeries, TsiValue}

/**
  * Created by dcollins on 1/14/17.
  */
class ModelSpec extends AbstractSpec {
  def extractValue[V: TsiValue](value: V): TsiValue[V]#T =
    implicitly[TsiValue[V]].value(value)

  "TsiValue" should "have an int constructor" in {
    val m = TsiValue(10)
    assert(m.x == 10)
    assert(extractValue(m) == 10)
  }

  it should "have a long constructor" in {
    val m = TsiValue(-1L)
    assert(m.x == -1L)
    assert(extractValue(m) == -1L)
  }

  it should "have a double constructor" in {
    val m = TsiValue(2.0)
    assert(m.x == 2.0)
    assert(extractValue(m) == 2.0)
  }

  it should "have a string constructor" in {
    val m = TsiValue("asdf")
    assert(m.x == "asdf")
    assert(extractValue(m) == "asdf")
  }

  "TsiMetric" should "have a constructor with no tags" in {
    val m = TsiMetric(1484428333, TsiValue(10L))()
  }

  it should "have a constructor with arbitrary tags" in {
    val m = TsiMetric(1484428333, TsiValue("Hello, world"))(
      "name" -> "hits",
      "path" -> "/home"
    )
  }

  "TsiSeries" should "have a constructor" in {
    val s = TsiSeries(
      TsiMetric(1484428300, TsiValue(10L))(),
      TsiMetric(1484428600, TsiValue(20L))(),
      TsiMetric(1484428900, TsiValue(30L))()
    )

    s.start shouldEqual 1484428300
  }


}
