package io.dac.tsindex.model

import TsiTypes._

/**
  * Created by dcollins on 1/14/17.
  */
case class TsiMetric[Value: TsiValue](timestamp: TsiTimestamp,
                                      value: Value,
                                      tags: Array[(String, String)])

object TsiMetric {
  def apply[Value: TsiValue](timestamp: TsiTimestamp, value: Value)(tags: (String, String)*): TsiMetric[Value] =
    TsiMetric(timestamp, value, tags.toArray)
}
