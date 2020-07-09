import java.text.SimpleDateFormat

import LogEvent.protocol.{ApacheLogEvent, UrlViewCount}
import LogEvent.winFun.{CountAgg, ProcessWindowFun, TopUrl}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LogEventApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val lineStream = env.readTextFile("E:\\study\\LogEvent\\src\\main\\resources\\apache.log").map(data => {
      val linearray = data.split(" ")
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = dateFormat.parse(linearray(3)).getTime
      ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
    }).keyBy(_.url).timeWindow(Time.seconds(60),Time.seconds(5))
    lineStream.aggregate(new CountAgg(),new ProcessWindowFun()).keyBy(_.windowEnd).process(new TopUrl(5)).print()
    env.execute("topUrl")
  }
}
