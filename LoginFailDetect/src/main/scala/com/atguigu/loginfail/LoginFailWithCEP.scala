package com.atguigu.loginfail

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {
  val n : Int = 2
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("Login.csv")
    // 1. 读取数据并转换成AdClickEvent
    val userLoginStream: KeyedStream[LoginEvent, Long] = env.readTextFile(resource.getPath)
      .map(line => {
        val items = line.split(",")
        LoginEvent(items(0).trim.toLong, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 乱序数据，设置延迟5秒
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
          override def extractTimestamp(e: LoginEvent): Long = e.eventTime * 1000L
        })
      .keyBy(_.userId)


    // 2. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 3. 在事件流上应用模式，得到Pattern Stream
    val patternStream = CEP.pattern(userLoginStream, loginFailPattern)

    // 4. 从Pattern Stream上应用Select Function，检测出匹配事件序列
    val loginFailStream = patternStream.select(new LoginFailMatch())

    loginFailStream.print("login fail with cep stream")
    env.execute("Login Fail With CEP")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中取出对应名称的事件
    val firstFail = map.get("begin").iterator().next()
    val nextFail = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, nextFail.eventTime, "login fail")
  }
}