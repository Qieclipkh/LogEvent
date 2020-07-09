package LogEvent.winFun

import LogEvent.protocol.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction


class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
