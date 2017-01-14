package io.dac.tsindex.model


/**
  * Created by dcollins on 1/14/17.
  */
sealed abstract class TsiValue[A] {
  type T
  def value: A => T
}

object TsiValue {
  case class TsiInt(x: Long)
    extends AnyVal

  case class TsiFloat(x: Double)
    extends AnyVal

  case class TsiString(x: String)
    extends AnyVal

  implicit object TsiInt extends TsiValue[TsiInt] {
    type T = Long
    override def value = _.x
  }
  implicit object TsiFlot extends TsiValue[TsiFloat] {
    type T = Double
    override def value = _.x
  }
  implicit object TsiString extends TsiValue[TsiString] {
    type T = String
    override def value = _.x
  }

  def apply(x: Long) = TsiInt(x)
  def apply(x: Int) = TsiInt(x.toLong)
  def apply(x: Double) = TsiFloat(x.toDouble)
  def apply(x: String) = TsiString(x)

}
