package com.atguigu.hotitems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object HotItemsKafka {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取Kafka中数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource(
      new FlinkKafkaConsumer[String]("user-behaviors", new SimpleStringSchema(), properties))
      .map(line => {
        val items: Array[String] = line.split(",")
        UserBehavior(
          items(0).trim.toLong, items(1).trim.toLong, items(2).trim.toInt, items(3).trim, items(4).trim.toLong)
      })
      // 由于是递增时间戳，使用递增timestamp即可
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. Transform过滤PV行为
    val itemViewCountStream = dataStream
      .filter(_.behavior == "pv")
      // 注意KEY的类型由keyBy决定，如果是string类型，key使用tuple，使用的是Long类型，key使用Long
      // .keyBy("itemId")
      .keyBy(_.itemId)
      // 进行开窗操作，1小时，5分钟滑动
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 窗口聚合
      .aggregate(new CountAgg(), new WindowResult())
      // 按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    // 4. Sink控制台输出
    itemViewCountStream.print()

    env.execute("Hot Item Analysis")
  }
}

