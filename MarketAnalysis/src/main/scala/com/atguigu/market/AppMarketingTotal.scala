package com.atguigu.market


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 基本需求
 * – 从埋点日志中，统计APP市场推广的数据指标
 * – 统计全量数据，不适合再用process function，避免处理全量数据，用aggregate增量聚合
 */
object AppMarketingTotal {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)

    val marketingTotalCntStream: DataStream[MarketingViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(_ => ("dummyKey", 1L))
      // 统计全量
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new WindowResult())

    marketingTotalCntStream.print("marketing total stream")
    env.execute("App Marketing Total")
  }
}

/**
 * 自定义预聚合函数，每次来一个数据，进行累加
 * IN 输入类型
 * ACC 中间累加变量
 * OUT 输出类型，交给WindowFunction
 **/
class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * 自定义窗口处理函数[IN, OUT, KEY, W]
 * 输入类型来自AggregateFunction
 * 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
 **/
class WindowResult() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[MarketingViewCount]): Unit = {
    val startTimestamp = window.getStart.toString
    val endTimestamp = window.getEnd.toString
    val cnt = input.iterator.next()
    out.collect(MarketingViewCount(startTimestamp, endTimestamp, "app", "total", cnt))
  }
}