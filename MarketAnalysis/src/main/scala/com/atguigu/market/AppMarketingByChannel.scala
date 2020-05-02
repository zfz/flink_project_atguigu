package com.atguigu.market

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 基本需求
 * – 从埋点日志中，统计APP市场推广的数据指标
 * – 按照不同的推广渠道，分别统计数据
 * 解决思路
 * – 通过过滤日志中的用户行为，按照不同的渠道进行统计
 * – 可以用 process function 处理，得到自定义的输出数据信息
 */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)

    val marketingByChannelStream: DataStream[MarketingViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(data => ((data.channel, data.behavior), 1L))
      // 以渠道和行为为key分组
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())

    marketingByChannelStream.print("marketing by channel stream")
    env.execute("App Marketing By Channel")
  }
}

/**
 * 自定义数据源
 */
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {
  // 是否运行的标志位
  var running = true
  // 用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 渠道集合
  val channelTypes: Seq[String] = Seq("Play Store", "AppStore")
  // 随机数发生器
  val rand: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    // 随机生成数据
    while (running && count < maxElements) {
      val userId = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelTypes(rand.nextInt(channelTypes.size))
      val ts = System.currentTimeMillis()

      sourceContext.collect(MarketingUserBehavior(userId, behavior, channel, ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10)
    }

  }

  override def cancel(): Unit = running = false
}

/**
 * 虽然keyBy过，但是经过了timeWindow，所以是ProcessWindowFunction
 * In: (data.channel, data.behavior), 1)
 * Out: MarketingViewCount
 * Key: (data.channel, data.behavior)
 * Window
 */
class MarketingCountByChannel()
  extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       ctx: Context,
                       elements: Iterable[((String, String), Long)],
                       out: Collector[MarketingViewCount]): Unit = {
    val startTimestamp = ctx.window.getStart.toString
    val endTimestamp = ctx.window.getEnd.toString
    out.collect(MarketingViewCount(startTimestamp, endTimestamp, key._1, key._2, elements.size))
  }
}