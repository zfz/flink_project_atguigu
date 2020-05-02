package com.atguigu.networkflow

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UVCount(windowEnd: Long, uvCount: Long)

/**
 * 使用set的方式进行过滤
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val inputPath = "HotItemAnalysis/src/main/resources/UserBehavior.csv"
    val dataStream = env.readTextFile(inputPath)
      .map(line => {
        val items = line.split(",")
        UserBehavior(
          items(0).trim.toLong, items(1).trim.toLong, items(2).trim.toInt, items(3).trim, items(4).trim.toLong)
      })
      // 由于是递增时间戳，使用递增timestamp即可
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream: DataStream[UVCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UVCountByWindow())

    uvStream.print("uv count")
    env.execute("Unique Visitor")
  }
}

/**
 * 没有进行key操作，同时对所有窗口内元素进行操作
 */
class UVCountByWindow() extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
    // 定义一个Set，保存所有的userId
    var idSet = Set[Long]()
    // 把当前窗口所有数据的ID收集到Set中，最后输出set的大小
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UVCount(window.getEnd, idSet.size))
  }
}