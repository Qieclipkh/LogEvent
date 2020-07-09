package LogEvent.protocol

case class UrlViewCount(url: String,
                        windowEnd: Long,
                        count: Long)

case class ApacheLogEvent(ip: String,
                          userId: String,
                          eventTime: Long,
                          method: String,
                          url: String)