package com.atguigu.loginfail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * LoginFail只能隔2秒之后去判断一下这期间是否有多次失败登录
 * 而不是在一次登录失败之后、再一次登录失败时就立刻报警
 * 这个需求要判断两次紧邻的事件，是否符合某种模式
 */
object LoginFailWithoutTimer {
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
      .process(new LoginWarningWithoutTimer())

    loginFailStream.print("login fail without timer stream")
    env.execute("Login Fail Without Timer")
  }
}

/**
 * 监控登录失败2次，如果连续登录失败，输出报警
 * 没法再用maxFailedTimes，这个函数仅判断2次失败
 * 对于多次失败，这样处理逻辑太复杂，更好的做法是使用CEP
 * 相比于LoginWarning，不再需要onTimer，事件间隔控制仍然是2秒，但是在processElement里处理
 */
class LoginWarningWithoutTimer() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 保存所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("log-fail-state",classOf[LoginEvent])
  )

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {
    if (value.eventType == "fail") {
      // 登录失败，判断前一次是否有登录失败
      val iters = loginFailState.get.iterator()
      // 已经有登录失败事件，就比较事件时间是否在2秒内
      if (iters.hasNext) {
        val firstFail = iters.next
        if (value.eventTime < firstFail.eventTime + 2) {
          // 如果两次间隔小于2秒，输出报警信息
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "Login fails in 2 seconds"))
        }
        // 更新最近一次登录事件，先clear再add比直接update操作简单
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 第一次登录失败，直接添加状态
        loginFailState.add(value)
      }
    } else {
      // 登录成功，清空状态并返回
      loginFailState.clear()
    }
  }
}