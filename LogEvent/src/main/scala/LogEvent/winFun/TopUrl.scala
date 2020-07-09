package LogEvent.winFun

import LogEvent.protocol.UrlViewCount
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopUrl(topN:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val listState=getRuntimeContext().getListState(new ListStateDescriptor[UrlViewCount]("map-stat",classOf[UrlViewCount]))

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    listState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val list=ListBuffer[UrlViewCount]()
    import scala.collection.JavaConversions._
    for(i<-listState.get()){
      list.append(i)
    }
    listState.clear()
    val urlViewCounts = list.sortBy(_.count)(Ordering.Long.reverse).take(topN)
    var stringBuilder=new StringBuilder()
    stringBuilder.append("time: ").append(timestamp-100)
    for(s<-urlViewCounts){
      stringBuilder.append("url: ").append(s.url).append(" count:").append(s.count).append("\n")
    }
    out.collect(stringBuilder.toString())
  }
}
