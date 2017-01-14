package io.dac.tsindex.model

import TsiTypes._

sealed abstract class TsiSeries[Value: TsiValue] {
  def start: TsiTimestamp
  def values: IndexedSeq[(TsiOffset, Value)]
}
object TsiSeries {
  case class RawSeries[Value: TsiValue](start: TsiTimestamp,
                                        values: Vector[(TsiOffset, Value)])
    extends TsiSeries

  def apply[Value: TsiValue](metrics: TsiMetric[Value]*): TsiSeries[Value] = {
    assert(metrics.nonEmpty)

    val start = metrics.head.timestamp
    val values = metrics.map { m =>
      ((m.timestamp - start).toInt, m.value)
    }.toVector

    RawSeries(start, values)
  }

}
