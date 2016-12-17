package io.dac.merkleindex

/**
  * Created by dcollins on 12/17/16.
  */
class TrivialSpec extends AbstractSpec {
  import MerkleApi._

  "An Index" should "return the values inserted" in {
    def program(implicit Api: MerkleApi.Dsl[Command]) =
      for {
        _ <- Api.add("hello", "10")
        v <- Api.get("hello")
      } yield v

    TrivialImpl.execute(program) shouldEqual "10"
  }
}
