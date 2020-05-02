package com.atguigu.loginfail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFail {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: Configuration = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = this.getClass.getClassLoader.getResource("Login.csv")
    // 读取数据并转换成AdClickEvent
    val loginStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(line => {
        val items = line.split(",")
        LoginEvent(items(0).trim.toLong, items(1).trim, items(2).trim, items(3).trim.toLong)
      })
      // 乱序数据，设置延迟5秒
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
          override def extractTimestamp(e: LoginEvent): Long = e.eventTime * 1000L
        })

    // 以用户ID分组
    val loginFailStream: DataStream[Warning] = loginStream
      .keyBy(_.userId)
      .process(new LoginWarning(2))

    loginFailStream.print("login fail stream")
    env.execute("Login Fail")
  }
}

/**
 * 监控2秒内的登录失败次数，检查到登录失败，设置2秒后的定时器
 * 这样处理的问题是2秒内发声大量登录失败的话，无法及时检测到
 * 类似于监控传感器温度连续上升的例子
 */
class LoginWarning(maxFailedTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 保存两秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("log-fail-state",classOf[LoginEvent])
  )

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              collector: Collector[Warning]): Unit = {
    // 登录失败，添加到loginFailState
    val loginFailList = loginFailState.get()

    if (value.eventType == "fail") {
      if (!loginFailList.iterator().hasNext()) {
        // 如果2s内有大量的失败
        ctx.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000L)
      }
      loginFailState.add(value)
    } else { // 登录成功，清空loginFailState状态
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext,
                       out: Collector[Warning]): Unit = {
    // 触发定时器时候，根据状态里的失败个数决定是否输出报警
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer()
    val iters = loginFailState.get().iterator()
    while (iters.hasNext) {
      allLoginFails += iters.next()
    }

    // 判断失败个数
    if (allLoginFails.size >= maxFailedTimes) {
      // ctx.getCurrentKey也可以获得用户ID
      out.collect(
        Warning(
          allLoginFails.head.userId,
          allLoginFails.head.eventTime,
          allLoginFails.last.eventTime,
          s"Login fails in 2 seconds ${allLoginFails.size} times"))
    }
    // 清空状态
    loginFailState.clear()
  }
}