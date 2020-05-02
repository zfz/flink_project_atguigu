package com.atguigu.orderpay


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchByJoin {
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

    // Join两条流
    val joinedStream = orderStream.intervalJoin(receiptStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    // 没法输出侧输出流，没法发送未匹配的报警信息
    joinedStream.print("tx join stream")

    env.execute("Tx Match By Join")
  }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(pay: OrderEvent,
                              receipt: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((pay, receipt))
  }
}