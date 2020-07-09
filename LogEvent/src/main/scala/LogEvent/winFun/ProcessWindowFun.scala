package LogEvent.winFun

import LogEvent.protocol.UrlViewCount
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class ProcessWindowFun extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,context.window.getEnd,elements.iterator.next()))
  }
}
