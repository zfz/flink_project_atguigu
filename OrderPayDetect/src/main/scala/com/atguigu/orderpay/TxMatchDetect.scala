package com.atguigu.orderpay

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TxMatchDetect {

  // 在Order中没有匹配到Receipt信息
  val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
  // 在Receipt中没有匹配到Order信息
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipt")

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单数据事件流
    val orderResource = this.getClass.getClassLoader.getResource("Order.csv")
    val orderStream: KeyedStream[OrderEvent, String] = env
      // 从7777端口访问，方便测试
      // .socketTextStream("localhost", 7777)
      .readTextFile(orderResource.getPath)
      .map(line => {
        val items = line.split(",")
        OrderEvent(items(0).trim, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 过滤非交易数据
      .filter(_.txId != "")
      // 升序数据
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取到帐数据事件流
    val receiptResource = this.getClass.getClassLoader.getResource("Receipt.csv")
    val receiptStream: KeyedStream[ReceiptEvent, String] = env
      // 从8888端口访问，方便测试
      // .socketTextStream("localhost", 8888)
      .readTextFile(receiptResource.getPath)
      .map(line => {
        val items = line.split(",")
        ReceiptEvent(items(0).trim, items(1).trim, items(2).trim.toLong)
      })
      // 升序数据
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 连接两条事件流
    val connectedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderStream
      .connect(receiptStream).process(new TxPayMatch())

    connectedStream.print("tx matched stream")
    connectedStream.getSideOutput(unmatchedPays).print("unmatched pay stream")
    connectedStream.getSideOutput(unmatchedReceipts).print("unmatched receipt stream")

    env.execute("Tx Match Detect")
  }

  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 保存订单支付状态
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))

    // 保存到帐状态
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 处理支付事件
    override def processElement1(pay: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到帐事件
      val receipt = receiptState.value()
      if (receipt != null) {
        // 如果已经有Receipt信息，在主流输出，并清空状态
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        // 如果还没有Receipt信息，把Pay存入状态，并注册定时器等待5秒
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000 + 51000)
      }
    }

    // 处理到帐事件
    override def processElement2(receipt: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay = payState.value()
      if (pay != null) {
        out.collect((pay, receipt))
        payState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000 + 5000)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 定时器触发，如果还没收到某个事件，输出报警
      if (payState.value() != null) {
        // Receipt没等到，输出Pay到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }

      if (receiptState.value() != null) {
        ctx.output(unmatchedReceipts, receiptState.value())
      }

      payState.clear()
      receiptState.clear()
    }
  }
}
