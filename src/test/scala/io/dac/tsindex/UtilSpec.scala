package io.dac.tsindex
import io.dac.tsindex.util.operators._

/**
  * Created by dcollins on 12/29/16.
  */
class UtilSpec extends AbstractSpec {
  "xor function" should "behave like the boolean xor" in {
    assert(false ^^ true)
    assert(true ^^ false)
    assert(! (true ^^ true))
    assert(! (false ^^ false))

  }

}
