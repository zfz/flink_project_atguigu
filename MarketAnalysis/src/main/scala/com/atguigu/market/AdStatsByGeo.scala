package com.atguigu.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AdStatsByGeo {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("AdClickLog.csv")
    // 读取数据并转换成AdClickEvent
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resource.getPath)
      .map(line => {
        val items = line.split(",")
        AdClickEvent(items(0).trim.toLong, items(1).trim.toLong, items(2).trim, items(3).trim, items(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 根据省份分组，开窗聚合
    val adCntByProvinceStream: DataStream[AdCountByProvince] = adEventStream
      .keyBy(_.province) // 如果需要聚合躲组，把需要聚合的Keys包装成元组
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdWindowResult())

    adCntByProvinceStream.print("ad count by prince stream")
    env.execute("Add Stats By Geo")
  }
}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdWindowResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}