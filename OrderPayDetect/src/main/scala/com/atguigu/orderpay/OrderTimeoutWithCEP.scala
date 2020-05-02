package com.atguigu.orderpay

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("Order.csv")
    // 1. 读取订单数据
    val orderStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
      .map(line => {
        val items = line.split(",")
        OrderEvent(items(0).trim, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 升序数据
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      // 宽松依赖，订单创建后可能还有其他操作（比如修改，添加）才支付
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 把模式应用到Stream上，得到Pattern Stream
    val patternStream = CEP.pattern(orderStream, orderPayPattern)

    // 4. 调用Select方法，提取事件序列，定义一个侧输出流标签，输出超时的报警信息
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("order-timeout")

    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeOutSelect(), new OrderPaySelect())

    resultStream.print("order payed stream")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("order timeout stream")

    env.execute("Order Timeout")
  }
}

/**
 * 自定义超时序列的自定义函数
 * In: OrderEvent
 * Out: OrderResult
 */
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {

  // 超时事件只有开始，没有结束
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // l参数是超时的时间戳
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, " order timeout")
  }
}

/**
 * 自定义正常序列的自定义函数
 * In: OrderEvent
 * Out: OrderResult
 */
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderPayId = map.get("follow").iterator().next().orderId
    OrderResult(orderPayId, " order payed successfully")
  }
}