package com.atguigu.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 订单超过15分钟没有下单，使用侧输出流输出信息
 */
object OrderTimeoutSideOutput {

  // 定义订单超时的侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("order-timeout")

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("Order.csv")
    // 1. 读取订单数据
    val orderStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
    // val orderStream: KeyedStream[OrderEvent, String] = env.socketTextStream("localhost", 7777)
      .map(line => {
        val items = line.split(",")
        OrderEvent(items(0).trim, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 升序数据
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义超时Process Function检测超时
    val orderResultStream = orderStream.process(new OrderPayMatch())
    orderResultStream.print("order payed stream")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("order timeout stream")

    env.execute("Order Timeout Side Output")
  }

  // 视频讲解的逻辑很乱
  class OrderPayMatch() extends KeyedProcessFunction[String, OrderEvent, OrderResult] {

    // 判断是否支付过
    lazy val isPayedState : ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("is-payed-state", classOf[Boolean])
    )

    // 保存定时器的时间戳
    lazy val timerState : ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer-state", classOf[Long])
    )

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[String, OrderEvent, OrderResult]#Context,
                                out: Collector[OrderResult]): Unit = {
      // 先读取状态
      val isPayed = isPayedState.value()
      val timerTS = timerState.value()

      // 处理Create事件
      if (value.eventType == "create") {
        if (isPayed) {
          isPayedState.clear()
          timerState.clear()
          return
        }
        val ts = value.eventTime * 1000 + 15 * 60 * 1000
        isPayedState.update(false)
        timerState.update(ts)
      }

      // 处理Pay事件
      if (value.eventType == "pay") {
        // timerTS == 0 没收到Create事件，不能判断null
        if (timerTS == 0 || value.eventTime * 1000 < timerTS) {
          out.collect(OrderResult(value.orderId, " order payed successfully"))
          isPayedState.update(true)
        } else {
          ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, " order timeout"))
          isPayedState.clear()
        }
        timerState.clear()
      }
    }

  }
}