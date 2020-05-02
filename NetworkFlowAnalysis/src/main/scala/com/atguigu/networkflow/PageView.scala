package com.atguigu.networkflow

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
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

    // 3. Transform过滤PV行为
    val pageViewCountStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      // 此处key的操作是为了开窗使用，哑key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    pageViewCountStream.print("page view count")

    env.execute("Page View")
  }
}
