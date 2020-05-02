package com.atguigu.hotitems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("user-behaviors")
  }

  def writeToKafka(topic: String) = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个Kafka Producer
    // 返回类型 [String, String]
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取数据
    val inputPath = "HotItemAnalysis/src/main/resources/UserBehavior.csv"
    val bufferedSource = scala.io.Source.fromFile(inputPath)
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
