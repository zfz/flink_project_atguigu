package com.atguigu.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 订单超过15分钟没有下单，输出信息
 */
object OrderTimeoutWithoutCEP {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单数据
    // val resource = this.getClass.getClassLoader.getResource("Order.csv")
    // val orderStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
    // 改成Socket方便调试，视频讲解的代码逻辑有误
    val orderStream: KeyedStream[OrderEvent, String] = env.socketTextStream("localhost", 7777)
      .map(line => {
        val items = line.split(",")
        OrderEvent(items(0).trim, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 升序数据
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义超时Process Function检测超时
    val timeoutWarningStream: DataStream[OrderResult] = orderStream.process(new OrderTimeOutWarning())

    timeoutWarningStream.print("timeout warning stream")
    env.execute("Order Timeout Without CEP")
  }
}

/**
 * 实现自定义处理函数，使用一个定时器，如果15分钟没有pay，输出超时信息
 */
class OrderTimeOutWarning() extends KeyedProcessFunction[String, OrderEvent, OrderResult] {

  // 判断是否支付过
  lazy val isPayedState : ValueState[Boolean] = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("is-payed-state", classOf[Boolean])
  )

  // 记录成功支付的时刻
  lazy val payedTSState : ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("payed-ts-state", classOf[Long])
  )

  override def processElement(value: OrderEvent,
                              ctx: KeyedProcessFunction[String, OrderEvent, OrderResult]#Context,
                              out: Collector[OrderResult]): Unit = {
    // 先取出状态标志位
    val isPayed = isPayedState.value()

    if (value.eventType == "create" && !isPayed) {
      // 如果遇到Create事件，并且Pay没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 15 * 60 * 1000)  // 超时定时器
    } else if (value.eventType == "pay") {
      // 如果是Pay事件，直接把状态改为True
      isPayedState.update(true)
      payedTSState.update(value.eventTime)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, OrderEvent, OrderResult]#OnTimerContext,
                       out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为True
    val isPayed = isPayedState.value()
    // 记录支付时刻
    val payedTS = payedTSState.value()

    // 视频讲解的代码逻辑有误，原来只判断if (isPayed)不够，需要添加一个State记录成功支付的时刻
    // 定时器触发时，除了判断是否支付，还需要判断支付时间是否超时
    if (isPayed && payedTS > timestamp) {
      out.collect(OrderResult(ctx.getCurrentKey, " order payed successfully"))
    } else {
      // 没用侧输出流
      out.collect(OrderResult(ctx.getCurrentKey, " order timeout"))
    }
    // 清空状态
    isPayedState.clear()
    payedTSState.clear()
  }
}